package utils

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/config"
	"RedisShake/internal/log"
)

type ClusterNodesSnapshot struct {
	Addresses []string
	Slots     [][]int
	AllNodes  []string
}

func GetRedisClusterSnapshotWithError(ctx context.Context, address string, username string, password string, Tls bool, tlsConfig client.TlsConfig, perferReplica bool) (ClusterNodesSnapshot, error) {
	return getRedisClusterNodesOnce(ctx, address, username, password, Tls, tlsConfig, perferReplica)
}

func GetRedisClusterNodes(ctx context.Context, address string, username string, password string, Tls bool, tlsConfig client.TlsConfig, perferReplica bool) (addresses []string, slots [][]int) {
	maxTimes := config.Opt.Advanced.IOReconnectMaxTimes
	if maxTimes <= 0 {
		maxTimes = 1
	}
	delay := time.Duration(config.Opt.Advanced.IOReconnectDelayMs) * time.Millisecond
	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			log.Panicf("get redis cluster nodes canceled. address=[%s]", address)
		default:
		}

		snapshot, err := getRedisClusterNodesOnce(ctx, address, username, password, Tls, tlsConfig, perferReplica)
		if err == nil {
			return snapshot.Addresses, snapshot.Slots
		}
		if !config.Opt.Advanced.IOReconnect || (!client.IsReconnectableIOError(err) && !client.IsRetryableRedisStateError(err)) {
			log.Panicf("get redis cluster nodes failed. address=[%s], err=[%v]", address, err)
		}

		roundAttempt := (attempt-1)%maxTimes + 1
		log.Warnf("reconnecting target cluster metadata. address=[%s], attempt=[%d/%d], delay=[%s], err=[%v]",
			address, roundAttempt, maxTimes, delay, err)
		if roundAttempt == maxTimes {
			log.Warnf("target cluster metadata reconnect attempts exhausted. continuing to retry until context is canceled. address=[%s]", address)
		}
		if delay > 0 {
			select {
			case <-ctx.Done():
				log.Panicf("get redis cluster nodes canceled. address=[%s]", address)
			case <-time.After(delay):
			}
		}
	}
}

func GetRedisClusterNodesFromAnyAddress(ctx context.Context, candidateAddresses []string, username string, password string, Tls bool, tlsConfig client.TlsConfig, perferReplica bool) (addresses []string, slots [][]int) {
	maxTimes := config.Opt.Advanced.IOReconnectMaxTimes
	if maxTimes <= 0 {
		maxTimes = 1
	}
	delay := time.Duration(config.Opt.Advanced.IOReconnectDelayMs) * time.Millisecond
	candidates := dedupeNonEmptyStrings(candidateAddresses)
	if len(candidates) == 0 {
		log.Panicf("get redis cluster nodes failed. no candidate addresses provided")
	}

	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			log.Panicf("get redis cluster nodes canceled. addresses=%v", candidates)
		default:
		}

		var lastRetryableErr error
		var fatalErr error
		for _, address := range candidates {
			snapshot, err := getRedisClusterNodesOnce(ctx, address, username, password, Tls, tlsConfig, perferReplica)
			if err == nil {
				return snapshot.Addresses, snapshot.Slots
			}
			if client.IsReconnectableIOError(err) || client.IsRetryableRedisStateError(err) {
				lastRetryableErr = err
				log.Warnf("reconnecting target cluster metadata via alternate candidate. address=[%s], err=[%v]", address, err)
				continue
			}
			fatalErr = err
			log.Warnf("target cluster metadata candidate failed with fatal error. address=[%s], err=[%v]", address, err)
		}

		if fatalErr != nil || !config.Opt.Advanced.IOReconnect {
			err := fatalErr
			if err == nil {
				err = lastRetryableErr
			}
			log.Panicf("get redis cluster nodes failed. addresses=%v, err=[%v]", candidates, err)
		}

		roundAttempt := (attempt-1)%maxTimes + 1
		log.Warnf("reconnecting target cluster metadata. addresses=%v, attempt=[%d/%d], delay=[%s], err=[%v]",
			candidates, roundAttempt, maxTimes, delay, lastRetryableErr)
		if roundAttempt == maxTimes {
			log.Warnf("target cluster metadata reconnect attempts exhausted. continuing to retry until context is canceled. addresses=%v", candidates)
		}
		if delay > 0 {
			select {
			case <-ctx.Done():
				log.Panicf("get redis cluster nodes canceled. addresses=%v", candidates)
			case <-time.After(delay):
			}
		}
	}
}

func GetRedisClusterSnapshotFromAnyAddress(ctx context.Context, candidateAddresses []string, username string, password string, Tls bool, tlsConfig client.TlsConfig, perferReplica bool) ClusterNodesSnapshot {
	maxTimes := config.Opt.Advanced.IOReconnectMaxTimes
	if maxTimes <= 0 {
		maxTimes = 1
	}
	delay := time.Duration(config.Opt.Advanced.IOReconnectDelayMs) * time.Millisecond
	candidates := dedupeNonEmptyStrings(candidateAddresses)
	if len(candidates) == 0 {
		log.Panicf("get redis cluster nodes failed. no candidate addresses provided")
	}

	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			log.Panicf("get redis cluster nodes canceled. addresses=%v", candidates)
		default:
		}

		var lastRetryableErr error
		var fatalErr error
		for _, address := range candidates {
			snapshot, err := getRedisClusterNodesOnce(ctx, address, username, password, Tls, tlsConfig, perferReplica)
			if err == nil {
				return snapshot
			}
			if client.IsReconnectableIOError(err) || client.IsRetryableRedisStateError(err) {
				lastRetryableErr = err
				log.Warnf("reconnecting target cluster metadata via alternate candidate. address=[%s], err=[%v]", address, err)
				continue
			}
			fatalErr = err
			log.Warnf("target cluster metadata candidate failed with fatal error. address=[%s], err=[%v]", address, err)
		}

		if fatalErr != nil || !config.Opt.Advanced.IOReconnect {
			err := fatalErr
			if err == nil {
				err = lastRetryableErr
			}
			log.Panicf("get redis cluster nodes failed. addresses=%v, err=[%v]", candidates, err)
		}

		roundAttempt := (attempt-1)%maxTimes + 1
		log.Warnf("reconnecting target cluster metadata. addresses=%v, attempt=[%d/%d], delay=[%s], err=[%v]",
			candidates, roundAttempt, maxTimes, delay, lastRetryableErr)
		if roundAttempt == maxTimes {
			log.Warnf("target cluster metadata reconnect attempts exhausted. continuing to retry until context is canceled. addresses=%v", candidates)
		}
		if delay > 0 {
			select {
			case <-ctx.Done():
				log.Panicf("get redis cluster nodes canceled. addresses=%v", candidates)
			case <-time.After(delay):
			}
		}
	}
}

func dedupeNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func getRedisClusterNodesOnce(ctx context.Context, address string, username string, password string, Tls bool, tlsConfig client.TlsConfig, perferReplica bool) (snapshot ClusterNodesSnapshot, err error) {
	c, err := client.NewRedisClientWithError(ctx, address, username, password, Tls, tlsConfig, false)
	if err != nil {
		return ClusterNodesSnapshot{}, err
	}
	defer c.Close()

	reply, err := c.DoWithStringReplyWithTimeout(3*time.Second, "cluster", "nodes")
	if err != nil {
		return ClusterNodesSnapshot{}, err
	}
	reply = strings.TrimSpace(reply)
	slotsCount := 0
	// map of master's nodeId to address
	masters := make(map[string]string)
	// map of master's nodeId to replica addresses
	replicas := make(map[string][]string)
	allNodes := make([]string, 0)
	// keep nodeID sort by slots
	nodeIds := make([]string, 0)
	nodeSlots := make([][]int, 0)
	log.Infof("address=%v, reply=%v", address, reply)
	for _, line := range strings.Split(reply, "\n") {
		line = strings.TrimSpace(line)
		words := strings.Split(line, " ")
		isMaster := strings.Contains(words[2], "master")
		if len(words) < 8 {
			return ClusterNodesSnapshot{}, errors.New("invalid cluster nodes line: " + line)
		}

		// address
		address := strings.Split(words[1], "@")[0]

		// handle ipv6 address
		tok := strings.Split(address, ":")
		if len(tok) > 2 {
			// ipv6 address
			port := tok[len(tok)-1]

			ipv6Addr := strings.Join(tok[:len(tok)-1], ":")
			address = fmt.Sprintf("[%s]:%s", ipv6Addr, port)
		}

		// handle hostname
		hostname := strings.Split(words[1], ",")
		if len(hostname) > 1 {
			address = fmt.Sprintf("%s:%s", hostname[1], tok[len(tok)-1])
		}
		allNodes = append(allNodes, address)

		if isMaster && len(words) < 9 {
			log.Warnf("the current master node does not hold any slots. address=[%v]", address)
			continue
		}

		nodeId := words[0]
		if isMaster {
			masters[nodeId] = address
		} else {
			// execlude invalid replicas node
			if strings.Contains(words[2], "fail") || strings.Contains(words[2], "noaddr") {
				continue
			}
			masterId := words[3]
			replicas[masterId] = append(replicas[masterId], address)
			continue
		}

		// parse slots
		slot := make([]int, 0)
		for i := 8; i < len(words); i++ {
			words[i] = strings.TrimSpace(words[i])
			if strings.HasPrefix(words[i], "[") {
				// issue: https://github.com/tair-opensource/RedisShake/issues/730
				// [****] appear at the end of each line of "cluster nodes",
				// indicating data migration between nodes is in progress.
				break
			}
			var start, end int
			var err error
			if strings.Contains(words[i], "-") {
				seg := strings.Split(words[i], "-")
				start, err = strconv.Atoi(seg[0])
				if err != nil {
					return ClusterNodesSnapshot{}, err
				}
				end, err = strconv.Atoi(seg[1])
				if err != nil {
					return ClusterNodesSnapshot{}, err
				}
			} else {
				start, err = strconv.Atoi(words[i])
				if err != nil {
					return ClusterNodesSnapshot{}, err
				}
				end = start
			}
			for j := start; j <= end; j++ {
				slot = append(slot, j)
				slotsCount++
			}
		}
		nodeSlots = append(nodeSlots, slot)
		nodeIds = append(nodeIds, nodeId)
	}
	if slotsCount != 16384 {
		return ClusterNodesSnapshot{}, fmt.Errorf("invalid cluster nodes slots. slots_count=%v, address=%v", slotsCount, address)
	}

	addresses := make([]string, 0, len(nodeIds))
	for _, id := range nodeIds {
		if replicaAddr, exist := replicas[id]; exist && perferReplica {
			addresses = append(addresses, replicaAddr[0])
		} else if masterAddr, exist := masters[id]; exist {
			addresses = append(addresses, masterAddr)
		} else {
			return ClusterNodesSnapshot{}, fmt.Errorf("unknown id=%s", id)
		}
	}

	return ClusterNodesSnapshot{
		Addresses: addresses,
		Slots:     nodeSlots,
		AllNodes:  dedupeNonEmptyStrings(allNodes),
	}, nil
}
