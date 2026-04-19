package writer

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterWriterShardsWithDocker(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") != "1" {
		t.Skip("set RUN_DOCKER_TESTS=1 to run Docker integration tests")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon is not available")
	}

	repoRoot := writerRepoRootFromPackageDir(t)
	imageName := writerDockerName(t.Name(), "img")
	buildImageWriter(t, imageName, repoRoot)

	network := writerDockerName(t.Name(), "net")
	clusterNodes := []string{
		writerDockerName(t.Name(), "cl-1"),
		writerDockerName(t.Name(), "cl-2"),
		writerDockerName(t.Name(), "cl-3"),
		writerDockerName(t.Name(), "cl-4"),
		writerDockerName(t.Name(), "cl-5"),
		writerDockerName(t.Name(), "cl-6"),
	}
	srcName := writerDockerName(t.Name(), "src")
	shakeName := writerDockerName(t.Name(), "shake")

	runDockerWriterIgnoreErr(append([]string{srcName, shakeName}, clusterNodes...)...)
	_ = exec.Command("docker", "network", "rm", network).Run()

	runDockerWriter(t, "network", "create", network)
	t.Cleanup(func() {
		runDockerWriterIgnoreErr(append([]string{srcName, shakeName}, clusterNodes...)...)
		_ = exec.Command("docker", "network", "rm", network).Run()
		_ = exec.Command("docker", "rmi", "-f", imageName).Run()
	})

	for _, node := range clusterNodes {
		runDockerWriter(t, "run", "-d", "--name", node, "--network", network,
			"redis:7.2-alpine", "redis-server",
			"--port", "6379",
			"--cluster-enabled", "yes",
			"--cluster-config-file", "nodes.conf",
			"--cluster-node-timeout", "5000",
			"--appendonly", "no",
			"--protected-mode", "no",
			"--bind", "0.0.0.0")
	}
	for _, node := range clusterNodes {
		waitRedisReadyWriter(t, node)
	}

	runDockerWriter(t, "exec", clusterNodes[0], "redis-cli", "--cluster", "create",
		clusterNodes[0]+":6379",
		clusterNodes[1]+":6379",
		clusterNodes[2]+":6379",
		clusterNodes[3]+":6379",
		clusterNodes[4]+":6379",
		clusterNodes[5]+":6379",
		"--cluster-replicas", "1", "--cluster-yes")
	waitClusterReadyWriter(t, clusterNodes[0])

	runDockerWriter(t, "run", "-d", "--name", srcName, "--network", network,
		"redis:7.2-alpine", "redis-server", "--appendonly", "no", "--protected-mode", "no", "--bind", "0.0.0.0")
	waitRedisReadyWriter(t, srcName)

	runDockerWriter(t, "exec", srcName, "redis-cli", "FLUSHALL")
	runDockerWriter(t, "exec", srcName, "redis-cli", "MSET", "k1", "v1", "k2", "v2")
	writeKeysViaPipeWriter(t, srcName, "bulk", 200)

	cfgPath := filepath.Join(t.TempDir(), "shake.toml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
[scan_reader]
cluster = false
address = "%s:6379"
username = ""
password = ""
tls = false
dbs = [0]
scan = true
ksn = false
count = 100
prefer_replica = false

[redis_writer]
cluster = true
address = "%s:6379"
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
ncpu = 2
pprof_port = 0
status_port = 0
log_file = "shake.log"
log_level = "info"
log_interval = 2
log_rotation = false
log_max_size = 8
log_max_age = 1
log_max_backups = 1
log_compress = false
io_reconnect = true
io_reconnect_max_times = 10
io_reconnect_delay_ms = 100
rdb_restore_command_behavior = "rewrite"
pipeline_count_limit = 1024
target_redis_max_qps = 200000
target_redis_oom_requeue = false
target_redis_oom_requeue_max_times = 3
target_redis_oom_requeue_delay_ms = 500
target_redis_writer_shards = 4
rewrite_collection_batch_size = 128
target_redis_client_max_querybuf_len = 67108864
target_redis_proto_max_bulk_len = 512000000
aws_psync = ""
empty_db_before_sync = true

[module]
target_mbbloom_version = 20603
`, srcName, clusterNodes[0], t.TempDir())), 0o644))

	runDockerWriter(t, "run", "-d", "--name", shakeName, "--network", network,
		"-v", cfgPath+":/work/shake.toml:ro",
		"--entrypoint", "/app/redis-shake",
		imageName, "/work/shake.toml")

	waitForLogInContainerWriter(t, shakeName, "start syncing...", 15*time.Second)
	waitForLogInContainerWriter(t, shakeName, "resolved_writer_shards=[1]", 15*time.Second)
	require.NotContains(t, dockerLogsWriter(t, shakeName), "redis writer sharding enabled. cluster=[true]")

	for _, key := range []string{"k1", "k2"} {
		waitForClusterKeyExistsWriter(t, clusterNodes[0], key, 20*time.Second, shakeName)
	}
	waitForClusterValueWriter(t, clusterNodes[0], "k1", "v1", 10*time.Second)
	waitForContainerExitWriter(t, shakeName, 20*time.Second)
	require.NotContains(t, dockerLogsWriter(t, shakeName), "panic:")
}

func TestClusterWriterFailoverReconnectWithDocker(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") != "1" {
		t.Skip("set RUN_DOCKER_TESTS=1 to run Docker integration tests")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon is not available")
	}

	repoRoot := writerRepoRootFromPackageDir(t)
	imageName := writerDockerName(t.Name(), "img")
	buildImageWriter(t, imageName, repoRoot)

	network := writerDockerName(t.Name(), "net")
	clusterNodes := []string{
		writerDockerName(t.Name(), "cl-1"),
		writerDockerName(t.Name(), "cl-2"),
		writerDockerName(t.Name(), "cl-3"),
		writerDockerName(t.Name(), "cl-4"),
		writerDockerName(t.Name(), "cl-5"),
		writerDockerName(t.Name(), "cl-6"),
	}
	srcName := writerDockerName(t.Name(), "src")
	shakeName := writerDockerName(t.Name(), "shake")

	runDockerWriterIgnoreErr(append([]string{srcName, shakeName}, clusterNodes...)...)
	_ = exec.Command("docker", "network", "rm", network).Run()

	runDockerWriter(t, "network", "create", network)
	t.Cleanup(func() {
		runDockerWriterIgnoreErr(append([]string{srcName, shakeName}, clusterNodes...)...)
		_ = exec.Command("docker", "network", "rm", network).Run()
		_ = exec.Command("docker", "rmi", "-f", imageName).Run()
	})

	for _, node := range clusterNodes {
		runDockerWriter(t, "run", "-d", "--name", node, "--network", network,
			"redis:7.2-alpine", "redis-server",
			"--port", "6379",
			"--cluster-enabled", "yes",
			"--cluster-config-file", "nodes.conf",
			"--cluster-node-timeout", "5000",
			"--appendonly", "no",
			"--protected-mode", "no",
			"--bind", "0.0.0.0")
	}
	for _, node := range clusterNodes {
		waitRedisReadyWriter(t, node)
	}

	runDockerWriter(t, "exec", clusterNodes[0], "redis-cli", "--cluster", "create",
		clusterNodes[0]+":6379",
		clusterNodes[1]+":6379",
		clusterNodes[2]+":6379",
		clusterNodes[3]+":6379",
		clusterNodes[4]+":6379",
		clusterNodes[5]+":6379",
		"--cluster-replicas", "1", "--cluster-yes")
	waitClusterReadyWriter(t, clusterNodes[0])

	runDockerWriter(t, "run", "-d", "--name", srcName, "--network", network,
		"redis:7.2-alpine", "redis-server",
		"--appendonly", "no",
		"--protected-mode", "no",
		"--bind", "0.0.0.0",
		"--notify-keyspace-events", "AKE")
	waitRedisReadyWriter(t, srcName)

	runDockerWriter(t, "exec", srcName, "redis-cli", "FLUSHALL")
	writeKeysViaPipeWriter(t, srcName, "bulk", 12000)

	cfgPath := filepath.Join(t.TempDir(), "shake.toml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
[scan_reader]
cluster = false
address = "%s:6379"
username = ""
password = ""
tls = false
dbs = [0]
scan = true
ksn = true
count = 64
prefer_replica = false

[redis_writer]
cluster = true
address = "%s:6379"
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
ncpu = 2
pprof_port = 0
status_port = 0
log_file = "shake.log"
log_level = "info"
log_interval = 2
log_rotation = false
log_max_size = 8
log_max_age = 1
log_max_backups = 1
log_compress = false
io_reconnect = true
io_reconnect_max_times = 10
io_reconnect_delay_ms = 100
rdb_restore_command_behavior = "rewrite"
pipeline_count_limit = 64
target_redis_max_qps = 200
target_redis_oom_requeue = false
target_redis_oom_requeue_max_times = 3
target_redis_oom_requeue_delay_ms = 500
target_redis_writer_shards = 1
rewrite_collection_batch_size = 128
target_redis_client_max_querybuf_len = 67108864
target_redis_proto_max_bulk_len = 512000000
aws_psync = ""
empty_db_before_sync = true

[module]
target_mbbloom_version = 20603
`, srcName, clusterNodes[0], t.TempDir())), 0o644))

	runDockerWriter(t, "run", "-d", "--name", shakeName, "--network", network,
		"-v", cfgPath+":/work/shake.toml:ro",
		"--entrypoint", "/app/redis-shake",
		imageName, "/work/shake.toml")

	waitForLogInContainerWriter(t, shakeName, "start syncing...", 15*time.Second)
	waitForClusterKeyExistsWriter(t, clusterNodes[0], "bulk:0", 20*time.Second, shakeName)

	masterID := clusterMyIDWriter(t, clusterNodes[0])
	replicaContainer := clusterReplicaContainerForMasterWriter(t, clusterNodes[0], masterID, clusterNodes)
	slotStart, slotEnd := clusterMyselfMasterSlotRangeWriter(t, clusterNodes[0])
	failoverKey := findKeyForSlotRangeWriter(t, clusterNodes[0], "failover-key", slotStart, slotEnd)

	runDockerWriter(t, "exec", replicaContainer, "redis-cli", "cluster", "failover", "takeover")
	waitForClusterRoleWriter(t, replicaContainer, "master", 20*time.Second)
	waitForClusterRoleWriter(t, clusterNodes[0], "slave", 20*time.Second)

	runDockerWriter(t, "exec", srcName, "redis-cli", "SET", failoverKey, "after-failover")
	waitForLogInContainerWriter(t, shakeName, "redis cluster topology refreshed.", 60*time.Second)
	waitForClusterKeyExistsWriter(t, clusterNodes[0], "bulk:11999", 120*time.Second, shakeName)

	logs := dockerLogsWriter(t, shakeName)
	require.NotContains(t, logs, "panic:")

	runDockerWriterIgnoreErr(shakeName)
}

func TestClusterWriterSeedNodeKilledReconnectWithDocker(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") != "1" {
		t.Skip("set RUN_DOCKER_TESTS=1 to run Docker integration tests")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon is not available")
	}

	repoRoot := writerRepoRootFromPackageDir(t)
	imageName := writerDockerName(t.Name(), "img")
	buildImageWriter(t, imageName, repoRoot)

	network := writerDockerName(t.Name(), "net")
	clusterNodes := []string{
		writerDockerName(t.Name(), "cl-1"),
		writerDockerName(t.Name(), "cl-2"),
		writerDockerName(t.Name(), "cl-3"),
		writerDockerName(t.Name(), "cl-4"),
		writerDockerName(t.Name(), "cl-5"),
		writerDockerName(t.Name(), "cl-6"),
	}
	srcName := writerDockerName(t.Name(), "src")
	shakeName := writerDockerName(t.Name(), "shake")

	runDockerWriterIgnoreErr(append([]string{srcName, shakeName}, clusterNodes...)...)
	_ = exec.Command("docker", "network", "rm", network).Run()

	runDockerWriter(t, "network", "create", network)
	t.Cleanup(func() {
		runDockerWriterIgnoreErr(append([]string{srcName, shakeName}, clusterNodes...)...)
		_ = exec.Command("docker", "network", "rm", network).Run()
		_ = exec.Command("docker", "rmi", "-f", imageName).Run()
	})

	for _, node := range clusterNodes {
		runDockerWriter(t, "run", "-d", "--name", node, "--network", network,
			"redis:7.2-alpine", "redis-server",
			"--port", "6379",
			"--cluster-enabled", "yes",
			"--cluster-config-file", "nodes.conf",
			"--cluster-node-timeout", "5000",
			"--appendonly", "no",
			"--protected-mode", "no",
			"--bind", "0.0.0.0")
	}
	for _, node := range clusterNodes {
		waitRedisReadyWriter(t, node)
	}

	runDockerWriter(t, "exec", clusterNodes[0], "redis-cli", "--cluster", "create",
		clusterNodes[0]+":6379",
		clusterNodes[1]+":6379",
		clusterNodes[2]+":6379",
		clusterNodes[3]+":6379",
		clusterNodes[4]+":6379",
		clusterNodes[5]+":6379",
		"--cluster-replicas", "1", "--cluster-yes")
	waitClusterReadyWriter(t, clusterNodes[0])

	runDockerWriter(t, "run", "-d", "--name", srcName, "--network", network,
		"redis:7.2-alpine", "redis-server",
		"--appendonly", "no",
		"--protected-mode", "no",
		"--bind", "0.0.0.0",
		"--notify-keyspace-events", "AKE")
	waitRedisReadyWriter(t, srcName)

	runDockerWriter(t, "exec", srcName, "redis-cli", "FLUSHALL")
	writeKeysViaPipeWriter(t, srcName, "bulk", 12000)

	cfgPath := filepath.Join(t.TempDir(), "shake.toml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
[scan_reader]
cluster = false
address = "%s:6379"
username = ""
password = ""
tls = false
dbs = [0]
scan = true
ksn = false
count = 64
prefer_replica = false

[redis_writer]
cluster = true
address = "%s:6379"
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
ncpu = 2
pprof_port = 0
status_port = 0
log_file = "shake.log"
log_level = "info"
log_interval = 2
log_rotation = false
log_max_size = 8
log_max_age = 1
log_max_backups = 1
log_compress = false
io_reconnect = true
io_reconnect_max_times = 10
io_reconnect_delay_ms = 100
rdb_restore_command_behavior = "rewrite"
pipeline_count_limit = 64
target_redis_max_qps = 200
target_redis_oom_requeue = false
target_redis_oom_requeue_max_times = 3
target_redis_oom_requeue_delay_ms = 500
target_redis_writer_shards = 1
rewrite_collection_batch_size = 128
target_redis_client_max_querybuf_len = 67108864
target_redis_proto_max_bulk_len = 512000000
aws_psync = ""
empty_db_before_sync = true

[module]
target_mbbloom_version = 20603
`, srcName, clusterNodes[0], t.TempDir())), 0o644))

	runDockerWriter(t, "run", "-d", "--name", shakeName, "--network", network,
		"-v", cfgPath+":/work/shake.toml:ro",
		"--entrypoint", "/app/redis-shake",
		imageName, "/work/shake.toml")

	waitForLogInContainerWriter(t, shakeName, "start syncing...", 15*time.Second)
	waitForClusterKeyExistsWriter(t, clusterNodes[0], "bulk:0", 20*time.Second, shakeName)

	runDockerWriterIgnoreErr(clusterNodes[0])

	masterID := clusterMyIDWriter(t, clusterNodes[1])
	replicaContainer := clusterReplicaContainerForMasterWriter(t, clusterNodes[1], masterID, clusterNodes)
	slotStart, slotEnd := clusterMyselfMasterSlotRangeWriter(t, clusterNodes[1])
	failoverKey := findKeyForSlotRangeWriter(t, clusterNodes[1], "seed-killed-key", slotStart, slotEnd)

	runDockerWriter(t, "exec", replicaContainer, "redis-cli", "cluster", "failover", "takeover")
	waitForClusterRoleWriter(t, replicaContainer, "master", 20*time.Second)
	waitForClusterRoleWriter(t, clusterNodes[1], "slave", 20*time.Second)
	waitForClusterSlotOwnerWriter(t, replicaContainer, replicaContainer, slotStart, slotEnd, 20*time.Second)

	runDockerWriter(t, "exec", srcName, "redis-cli", "SET", failoverKey, "after-seed-killed")
	waitForLogInContainerWriter(t, shakeName, "redis cluster topology refreshed.", 60*time.Second)
	waitForClusterKeyExistsWriter(t, replicaContainer, "bulk:11999", 120*time.Second, shakeName)

	logs := dockerLogsWriter(t, shakeName)
	require.NotContains(t, logs, "panic:")

	runDockerWriterIgnoreErr(shakeName)
}

// TestClusterWriterTwoNodeFailoverWithDocker tests the 2-node cluster scenario
// that exposed the topology candidate bug in production:
//
//   - Cluster has exactly 1 master (A) + 1 slave (B)
//   - RedisShake is configured with A's address
//   - A is killed hard (process killed, no TCP)
//   - B is elected master
//   - RedisShake must switch to B and continue syncing
//
// Previously, after A was killed and B's write connection had a brief error
// during the leadership transition, `topologyCandidatesLocked` excluded B
// (the failedWriter) from topology candidates. With only A remaining and A
// being dead, all topology queries failed → panic after timeout.
func TestClusterWriterTwoNodeFailoverWithDocker(t *testing.T) {
	if os.Getenv("RUN_DOCKER_TESTS") != "1" {
		t.Skip("set RUN_DOCKER_TESTS=1 to run Docker integration tests")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon is not available")
	}

	repoRoot := writerRepoRootFromPackageDir(t)
	imageName := writerDockerName(t.Name(), "img")
	buildImageWriter(t, imageName, repoRoot)

	network := writerDockerName(t.Name(), "net")
	// 2-node cluster: one master, one slave
	nodeA := writerDockerName(t.Name(), "node-a") // initial master
	nodeB := writerDockerName(t.Name(), "node-b") // initial slave → new master after kill
	srcName := writerDockerName(t.Name(), "src")
	shakeName := writerDockerName(t.Name(), "shake")

	runDockerWriterIgnoreErr(srcName, shakeName, nodeA, nodeB)
	_ = exec.Command("docker", "network", "rm", network).Run()

	runDockerWriter(t, "network", "create", network)
	t.Cleanup(func() {
		runDockerWriterIgnoreErr(srcName, shakeName, nodeA, nodeB)
		_ = exec.Command("docker", "network", "rm", network).Run()
		_ = exec.Command("docker", "rmi", "-f", imageName).Run()
	})

	for _, node := range []string{nodeA, nodeB} {
		runDockerWriter(t, "run", "-d", "--name", node, "--network", network,
			"redis:7.2-alpine", "redis-server",
			"--port", "6379",
			"--cluster-enabled", "yes",
			"--cluster-config-file", "nodes.conf",
			"--cluster-node-timeout", "3000",
			"--appendonly", "no",
			"--protected-mode", "no",
			"--bind", "0.0.0.0")
	}
	waitRedisReadyWriter(t, nodeA)
	waitRedisReadyWriter(t, nodeB)

	// Create a 2-node cluster: 1 master + 1 replica
	runDockerWriter(t, "exec", nodeA, "redis-cli", "--cluster", "create",
		nodeA+":6379", nodeB+":6379",
		"--cluster-replicas", "0", "--cluster-yes")
	waitClusterReadyWriter(t, nodeA)

	// Add nodeB as replica of nodeA
	nodeAID := clusterMyIDWriter(t, nodeA)
	runDockerWriter(t, "exec", nodeB, "redis-cli", "cluster", "replicate", nodeAID)

	// Wait for nodeB to become a replica
	waitForClusterRoleWriter(t, nodeB, "slave", 15*time.Second)

	// Source redis (standalone) with KSN for live sync
	runDockerWriter(t, "run", "-d", "--name", srcName, "--network", network,
		"redis:7.2-alpine", "redis-server",
		"--appendonly", "no", "--protected-mode", "no", "--bind", "0.0.0.0",
		"--notify-keyspace-events", "AKE")
	waitRedisReadyWriter(t, srcName)

	writeKeysViaPipeWriter(t, srcName, "pre", 500)

	cfgPath := writerWriteTwoNodeConfig(t, srcName, nodeA)

	runDockerWriter(t, "run", "-d", "--name", shakeName, "--network", network,
		"-v", cfgPath+":/work/shake.toml:ro",
		"--entrypoint", "/app/redis-shake",
		imageName, "/work/shake.toml")

	waitForLogInContainerWriter(t, shakeName, "start syncing...", 20*time.Second)
	waitForClusterKeyExistsWriter(t, nodeA, "pre:0", 20*time.Second, shakeName)

	// Find a key that lands in nodeA's slot range so we can write it after failover
	slotStart, slotEnd := clusterMyselfMasterSlotRangeWriter(t, nodeA)
	failoverKey := findKeyForSlotRangeWriter(t, nodeA, "post-failover", slotStart, slotEnd)

	// Kill nodeA hard — no graceful shutdown, no TCP
	runDockerWriterIgnoreErr(nodeA)

	// Wait for nodeB to become master
	waitForClusterRoleWriter(t, nodeB, "master", 20*time.Second)

	// Write a new key to the source; RedisShake must sync it through nodeB
	runDockerWriter(t, "exec", srcName, "redis-cli", "SET", failoverKey, "after-kill")

	// RedisShake must detect the topology change and reconnect to nodeB
	waitForLogInContainerWriter(t, shakeName, "redis cluster topology refreshed.", 60*time.Second)

	// Verify the post-failover key was synced to nodeB
	waitForClusterKeyExistsWriter(t, nodeB, failoverKey, 30*time.Second, shakeName)

	logs := dockerLogsWriter(t, shakeName)
	require.NotContains(t, logs, "panic:")
}

func writerWriteTwoNodeConfig(t *testing.T, srcName, masterNode string) string {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "shake.toml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
[scan_reader]
cluster = false
address = "%s:6379"
username = ""
password = ""
tls = false
dbs = [0]
scan = true
ksn = true
count = 64
prefer_replica = false

[redis_writer]
cluster = true
address = "%s:6379"
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
ncpu = 2
pprof_port = 0
status_port = 0
log_file = "shake.log"
log_level = "info"
log_interval = 2
log_rotation = false
log_max_size = 8
log_max_age = 1
log_max_backups = 1
log_compress = false
io_reconnect = true
io_reconnect_max_times = 20
io_reconnect_delay_ms = 500
rdb_restore_command_behavior = "rewrite"
pipeline_count_limit = 64
target_redis_max_qps = 50000
target_redis_oom_requeue = false
target_redis_oom_requeue_max_times = 3
target_redis_oom_requeue_delay_ms = 500
rewrite_collection_batch_size = 128
target_redis_client_max_querybuf_len = 67108864
target_redis_proto_max_bulk_len = 512000000
aws_psync = ""
empty_db_before_sync = true

[module]
target_mbbloom_version = 20603
`, srcName, masterNode, t.TempDir())), 0o644))
	return cfgPath
}

func writerRepoRootFromPackageDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func writerDockerName(testName, suffix string) string {
	s := strings.ToLower(testName)
	s = strings.NewReplacer("/", "-", "_", "-", " ", "-").Replace(s)
	base := "redisshake-" + s
	if len(base) > 40 {
		base = base[:40]
		base = strings.TrimRight(base, "-")
	}
	return base + "-" + suffix
}

func runDockerWriter(t *testing.T, args ...string) string {
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

func runDockerWriterIgnoreErr(names ...string) {
	args := append([]string{"rm", "-f"}, names...)
	_ = exec.Command("docker", args...).Run()
}

func writeKeysViaPipeWriter(t *testing.T, container, prefix string, count int) {
	t.Helper()
	cmd := exec.Command("docker", "exec", "-i", container, "redis-cli", "--pipe")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())

	for i := 0; i < count; i++ {
		_, err = fmt.Fprintf(stdin, "SET %s:%d %d\n", prefix, i, i)
		require.NoError(t, err)
	}
	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait(), "redis-cli --pipe failed: stdout=%s stderr=%s", stdout.String(), stderr.String())
}

func buildImageWriter(t *testing.T, imageName, repoRoot string) {
	t.Helper()
	cmd := exec.Command("docker", "build", "-t", imageName, ".")
	cmd.Dir = repoRoot
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		t.Fatalf("docker build image=%s failed: %v, stdout=%s stderr=%s", imageName, err, stdout.String(), stderr.String())
	}
}

func waitRedisReadyWriter(t *testing.T, container string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
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

func waitClusterReadyWriter(t *testing.T, container string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", container, "redis-cli", "cluster", "info")
		out, err := cmd.CombinedOutput()
		if err == nil && strings.Contains(string(out), "cluster_state:ok") {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("redis cluster %s was not ready before timeout", container)
}

func waitForLogInContainerWriter(t *testing.T, container, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		logs := dockerLogsWriter(t, container)
		if strings.Contains(logs, needle) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("did not find log %q within %s; logs=%s", needle, timeout, dockerLogsWriter(t, container))
}

func dockerLogsWriter(t *testing.T, container string) string {
	t.Helper()
	cmd := exec.Command("docker", "logs", container)
	out, _ := cmd.CombinedOutput()
	return string(out)
}

func waitForClusterValueWriter(t *testing.T, container, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", container, "redis-cli", "-c", "--raw", "GET", key)
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("cluster key %s did not reach expected value %q within %s", key, want, timeout)
}

func waitForClusterKeyExistsWriter(t *testing.T, container, key string, timeout time.Duration, debugLogsContainer string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", container, "redis-cli", "-c", "--raw", "EXISTS", key)
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "1" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("cluster key %s did not exist within %s; redis-shake logs=%s", key, timeout, dockerLogsWriter(t, debugLogsContainer))
}

func waitForContainerExitWriter(t *testing.T, container string, timeout time.Duration) {
	t.Helper()
	done := make(chan string, 1)
	go func() {
		cmd := exec.Command("docker", "wait", container)
		out, _ := cmd.CombinedOutput()
		done <- strings.TrimSpace(string(out))
	}()
	select {
	case code := <-done:
		require.Equal(t, "0", code, "container exited abnormally, logs=%s", dockerLogsWriter(t, container))
	case <-time.After(timeout):
		t.Fatalf("redis-shake container did not exit within %s", timeout)
	}
}

func clusterMyIDWriter(t *testing.T, container string) string {
	t.Helper()
	cmd := exec.Command("docker", "exec", container, "redis-cli", "--raw", "cluster", "myid")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "cluster myid failed: %s", string(out))
	return strings.TrimSpace(string(out))
}

func clusterReplicaContainerForMasterWriter(t *testing.T, queryContainer, masterID string, clusterNodes []string) string {
	t.Helper()
	cmd := exec.Command("docker", "exec", queryContainer, "redis-cli", "--raw", "cluster", "replicas", masterID)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "cluster replicas failed: %s", string(out))
	ips := make(map[string]string, len(clusterNodes))
	for _, node := range clusterNodes {
		ip := containerIPAddressWriterIfExistsWriter(node)
		if ip == "" {
			continue
		}
		ips[ip] = node
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		host := strings.Split(strings.Split(fields[1], "@")[0], ":")[0]
		if node, ok := ips[host]; ok {
			return node
		}
	}
	t.Fatalf("replica for master %s not found in output: %s", masterID, string(out))
	return ""
}

func clusterMyselfMasterSlotRangeWriter(t *testing.T, container string) (int, int) {
	t.Helper()
	cmd := exec.Command("docker", "exec", container, "redis-cli", "--raw", "cluster", "nodes")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "cluster nodes failed: %s", string(out))
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 9 {
			continue
		}
		if !strings.Contains(fields[2], "myself,master") {
			continue
		}
		rangeFields := strings.Split(fields[8], "-")
		require.Len(t, rangeFields, 2, "unexpected slot range: %s", fields[8])
		start := mustAtoiWriter(t, rangeFields[0])
		end := mustAtoiWriter(t, rangeFields[1])
		return start, end
	}
	t.Fatalf("myself master slot range not found in %s", string(out))
	return 0, 0
}

func findKeyForSlotRangeWriter(t *testing.T, container, prefix string, start, end int) string {
	t.Helper()
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("%s:%d", prefix, i)
		cmd := exec.Command("docker", "exec", container, "redis-cli", "--raw", "cluster", "keyslot", key)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "cluster keyslot failed: %s", string(out))
		slot := mustAtoiWriter(t, strings.TrimSpace(string(out)))
		if slot >= start && slot <= end {
			return key
		}
	}
	t.Fatalf("failed to find key for slot range %d-%d", start, end)
	return ""
}

func waitForClusterRoleWriter(t *testing.T, container, role string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", container, "redis-cli", "--raw", "cluster", "nodes")
		out, err := cmd.CombinedOutput()
		if err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				fields := strings.Fields(strings.TrimSpace(line))
				if len(fields) >= 3 && strings.Contains(fields[2], "myself,"+role) {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("container %s did not become %s within %s", container, role, timeout)
}

func waitForDirectValueWriter(t *testing.T, container, key, want string, timeout time.Duration) {
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
	t.Fatalf("direct key %s on %s did not reach expected value %q within %s", key, container, want, timeout)
}

func mustAtoiWriter(t *testing.T, s string) int {
	t.Helper()
	v, err := strconv.Atoi(strings.TrimSpace(s))
	require.NoError(t, err)
	return v
}

func containerIPAddressWriter(t *testing.T, container string) string {
	t.Helper()
	ip := containerIPAddressWriterIfExistsWriter(container)
	require.NotEmpty(t, ip, "inspect ip failed for %s", container)
	return ip
}

func containerIPAddressWriterIfExistsWriter(container string) string {
	cmd := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func waitForClusterSlotOwnerWriter(t *testing.T, queryContainer, ownerContainer string, start, end int, timeout time.Duration) {
	t.Helper()
	ownerIP := containerIPAddressWriter(t, ownerContainer)
	wantSlotRange := fmt.Sprintf("%d-%d", start, end)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", queryContainer, "redis-cli", "--raw", "cluster", "nodes")
		out, err := cmd.CombinedOutput()
		if err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				fields := strings.Fields(strings.TrimSpace(line))
				if len(fields) < 9 {
					continue
				}
				host := strings.Split(strings.Split(fields[1], "@")[0], ":")[0]
				if host == ownerIP && strings.Contains(fields[2], "master") && strings.Contains(line, wantSlotRange) {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("container %s did not own slot range %s within %s when queried from %s", ownerContainer, wantSlotRange, timeout, queryContainer)
}
