package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIOReconnectWithDocker(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") != "1" {
		t.Skip("set RUN_DOCKER_TESTS=1 to run Docker integration tests")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	dockerCmd := exec.Command("docker", "info")
	if err := dockerCmd.Run(); err != nil {
		t.Skip("docker daemon is not available")
	}

	repoRoot := repoRootFromPackageDir(t)
	binary := filepath.Join(t.TempDir(), "redis-shake")
	build := exec.Command("go", "build", "-o", binary, "./cmd/redis-shake")
	build.Dir = repoRoot
	require.NoError(t, build.Run())

	t.Run("ksn_source_and_target_reconnect", func(t *testing.T) {
		srcName := dockerName(t.Name(), "src")
		dstName := dockerName(t.Name(), "dst")
		srcPort := freeTCPPort(t)
		dstPort := freeTCPPort(t)

		runDockerIgnoreErr(srcName, dstName)
		runDocker(t, "run", "-d", "--name", srcName, "-p", srcPort+":6379",
			"redis:7", "redis-server", "--save", "", "--appendonly", "no", "--notify-keyspace-events", "AKE")
		t.Cleanup(func() { runDockerIgnoreErr(srcName, dstName) })
		runDocker(t, "run", "-d", "--name", dstName, "-p", dstPort+":6379",
			"redis:7", "redis-server", "--save", "", "--appendonly", "no")

		waitRedisReady(t, srcName)
		waitRedisReady(t, dstName)
		runDocker(t, "exec", srcName, "redis-cli", "FLUSHALL")
		runDocker(t, "exec", dstName, "redis-cli", "FLUSHALL")

		cfgPath := filepath.Join(t.TempDir(), "shake.toml")
		require.NoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
[scan_reader]
cluster = false
address = "127.0.0.1:%s"
username = ""
password = ""
tls = false
dbs = [0]
scan = false
ksn = true
count = 10
prefer_replica = false

[redis_writer]
cluster = false
address = "127.0.0.1:%s"
username = ""
password = ""
tls = false
off_reply = false

[filter]
allow_keys = []
allow_key_prefix = []
allow_key_suffix = []
allow_key_regex = []
block_keys = []
block_key_prefix = []
block_key_suffix = []
block_key_regex = []
allow_db = []
block_db = []
allow_command = []
block_command = []
allow_command_group = []
block_command_group = []
function = ""

[advanced]
dir = "%s"
ncpu = 1
pprof_port = 0
status_port = 0
log_file = "shake.log"
log_level = "info"
log_interval = 5
log_rotation = false
log_max_size = 8
log_max_age = 1
log_max_backups = 1
log_compress = false
io_reconnect = true
io_reconnect_max_times = 20
io_reconnect_delay_ms = 100
rdb_restore_command_behavior = "rewrite"
pipeline_count_limit = 64
target_redis_max_qps = 100000
target_redis_oom_requeue = false
target_redis_oom_requeue_max_times = 3
target_redis_oom_requeue_delay_ms = 500
target_redis_client_max_querybuf_len = 67108864
target_redis_proto_max_bulk_len = 512000000
aws_psync = ""
empty_db_before_sync = false

[module]
target_mbbloom_version = 20603
`, srcPort, dstPort, t.TempDir())), 0o644))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, cfgPath)
		cmd.Dir = repoRoot
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		require.NoError(t, cmd.Start())
		t.Cleanup(func() {
			cancel()
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		})

		waitForLog(t, &output, "start syncing...", 10*time.Second)

		runDocker(t, "exec", srcName, "redis-cli", "SET", "k1", "v1")
		waitForRedisValue(t, dstName, "k1", "v1", 10*time.Second)

		runDocker(t, "stop", dstName)
		runDocker(t, "exec", srcName, "redis-cli", "SET", "k2", "v2")
		time.Sleep(500 * time.Millisecond)
		runDocker(t, "start", dstName)
		waitRedisReady(t, dstName)
		waitForLog(t, &output, "reconnected target redis", 10*time.Second)
		waitForRedisValue(t, dstName, "k2", "v2", 10*time.Second)

		runDocker(t, "stop", srcName)
		time.Sleep(500 * time.Millisecond)
		runDocker(t, "start", srcName)
		waitRedisReady(t, srcName)
		waitForLog(t, &output, "reconnected source redis", 10*time.Second)
		runDocker(t, "exec", srcName, "redis-cli", "SET", "k3", "v3")
		waitForRedisValue(t, dstName, "k3", "v3", 10*time.Second)
	})

	t.Run("ksn_target_reconnect_with_backlog", func(t *testing.T) {
		srcName := dockerName(t.Name(), "src")
		dstName := dockerName(t.Name(), "dst")
		srcPort := freeTCPPort(t)
		dstPort := freeTCPPort(t)

		runDockerIgnoreErr(srcName, dstName)
		runDocker(t, "run", "-d", "--name", srcName, "-p", srcPort+":6379",
			"redis:7", "redis-server", "--save", "", "--appendonly", "no", "--notify-keyspace-events", "AKE")
		t.Cleanup(func() { runDockerIgnoreErr(srcName, dstName) })
		runDocker(t, "run", "-d", "--name", dstName, "-p", dstPort+":6379",
			"redis:7", "redis-server", "--save", "", "--appendonly", "no")

		waitRedisReady(t, srcName)
		waitRedisReady(t, dstName)
		runDocker(t, "exec", srcName, "redis-cli", "FLUSHALL")
		runDocker(t, "exec", dstName, "redis-cli", "FLUSHALL")

		cfgPath := filepath.Join(t.TempDir(), "shake.toml")
		require.NoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
[scan_reader]
cluster = false
address = "127.0.0.1:%s"
username = ""
password = ""
tls = false
dbs = [0]
scan = false
ksn = true
count = 100
prefer_replica = false

[redis_writer]
cluster = false
address = "127.0.0.1:%s"
username = ""
password = ""
tls = false
off_reply = false

[filter]
allow_keys = []
allow_key_prefix = []
allow_key_suffix = []
allow_key_regex = []
block_keys = []
block_key_prefix = []
block_key_suffix = []
block_key_regex = []
allow_db = []
block_db = []
allow_command = []
block_command = []
allow_command_group = []
block_command_group = []
function = ""

[advanced]
dir = "%s"
ncpu = 1
pprof_port = 0
status_port = 0
log_file = "shake.log"
log_level = "info"
log_interval = 1
log_rotation = false
log_max_size = 8
log_max_age = 1
log_max_backups = 1
log_compress = false
io_reconnect = true
io_reconnect_max_times = 50
io_reconnect_delay_ms = 100
rdb_restore_command_behavior = "rewrite"
pipeline_count_limit = 64
target_redis_max_qps = 100000
target_redis_oom_requeue = false
target_redis_oom_requeue_max_times = 3
target_redis_oom_requeue_delay_ms = 500
target_redis_client_max_querybuf_len = 67108864
target_redis_proto_max_bulk_len = 512000000
aws_psync = ""
empty_db_before_sync = false

[module]
target_mbbloom_version = 20603
`, srcPort, dstPort, t.TempDir())), 0o644))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, cfgPath)
		cmd.Dir = repoRoot
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		require.NoError(t, cmd.Start())
		t.Cleanup(func() {
			cancel()
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		})

		waitForLog(t, &output, "start syncing...", 10*time.Second)

		runDocker(t, "stop", dstName)
		writeKeysViaPipe(t, srcName, "backlog", 1500)
		time.Sleep(1 * time.Second)
		runDocker(t, "start", dstName)
		waitRedisReady(t, dstName)
		waitForLog(t, &output, "reconnected target redis", 15*time.Second)
		waitForRedisDBSizeWithLogs(t, dstName, 1500, 30*time.Second, &output)

		writeKeysViaPipe(t, srcName, "postreconnect", 100)
		waitForRedisDBSizeWithLogs(t, dstName, 1600, 30*time.Second, &output)
		waitForRedisValue(t, dstName, "postreconnect:99", "99", 10*time.Second)
	})
}

func freeTCPPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port)
}

func repoRootFromPackageDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func dockerName(testName, suffix string) string {
	s := strings.ToLower(testName)
	s = strings.NewReplacer("/", "-", "_", "-", " ", "-").Replace(s)
	return "redisshake-" + s + "-" + suffix
}

func runDocker(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		t.Fatalf("docker %s failed: %v, stdout=%s stderr=%s", strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return strings.TrimSpace(stdout.String())
}

func runDockerIgnoreErr(names ...string) {
	args := append([]string{"rm", "-f"}, names...)
	_ = exec.Command("docker", args...).Run()
}

func writeKeysViaPipe(t *testing.T, container, prefix string, count int) {
	t.Helper()
	cmd := exec.Command("docker", "exec", "-i", container, "redis-cli", "--pipe")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())

	for i := 0; i < count; i++ {
		_, err = io.WriteString(stdin, fmt.Sprintf("SET %s:%d %d\n", prefix, i, i))
		require.NoError(t, err)
	}
	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait(), "redis-cli --pipe failed: stdout=%s stderr=%s", stdout.String(), stderr.String())
}

func waitRedisReady(t *testing.T, container string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", container, "redis-cli", "PING")
		out, err := cmd.CombinedOutput()
		if err == nil && strings.Contains(string(out), "PONG") {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("redis %s was not ready before timeout", container)
}

func waitForRedisValue(t *testing.T, container, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", container, "redis-cli", "--raw", "GET", key)
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("redis key %s did not reach expected value %q within %s", key, want, timeout)
}

func waitForRedisDBSize(t *testing.T, container string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", container, "redis-cli", "--raw", "DBSIZE")
		out, err := cmd.CombinedOutput()
		if err == nil {
			got, convErr := strconv.Atoi(strings.TrimSpace(string(out)))
			if convErr == nil && got == want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("redis dbsize in %s did not reach %d within %s", container, want, timeout)
}

func waitForRedisDBSizeWithLogs(t *testing.T, container string, want int, timeout time.Duration, logs *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", container, "redis-cli", "--raw", "DBSIZE")
		out, err := cmd.CombinedOutput()
		if err == nil {
			got, convErr := strconv.Atoi(strings.TrimSpace(string(out)))
			if convErr == nil && got == want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("redis dbsize in %s did not reach %d within %s; redis-shake logs=%s", container, want, timeout, logs.String())
}

func waitForLog(t *testing.T, buf *bytes.Buffer, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), needle) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("did not find log %q within %s; logs=%s", needle, timeout, buf.String())
}
