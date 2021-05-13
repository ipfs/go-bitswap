package fuzz

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Zap's timestamp format
// https://github.com/uber-go/zap/blob/fa2c78c024dc1f1481fd9940f2d85f7cc8450cd9/zapcore/encoder.go#L128
var TimestampFormat = "2006-01-02T15:04:05.000Z0700"

func checkLogFileInvariants(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	logs := make([]map[string]interface{}, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var log map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &log); err != nil {
			return err
		}

		t, err := time.Parse(TimestampFormat, log["ts"].(string))
		if err != nil {
			return err
		}
		log["ts"] = t
		logs = append(logs, log)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return checkInvariants(logs)
}

func checkInvariants(logs []map[string]interface{}) error {
	if err := checkWantBlockCancel(logs); err != nil {
		return err
	}

	if err := checkWantSentOnce(logs); err != nil {
		return err
	}

	if err := checkTimeoutAfterBlockReceived(logs); err != nil {
		return err
	}

	return nil
}

type lineAt struct {
	line int
	at   time.Time
}

func checkWantBlockCancel(logs []map[string]interface{}) error {
	// node id => cid => timestamp
	rcvdBlocks := make(map[string]map[string]*lineAt)
	for lineIdx, log := range logs {
		if !strings.Contains(log["msg"].(string), "Bitswap <- block") {
			continue
		}

		pid := log["local"].(string)
		at := log["ts"].(time.Time)
		c := log["cid"].(string)

		peerBlks, ok := rcvdBlocks[pid]
		if !ok {
			peerBlks = make(map[string]*lineAt)
			rcvdBlocks[pid] = peerBlks
		}

		blkAt, ok := peerBlks[c]
		if !ok || at.Before(blkAt.at) {
			peerBlks[c] = &lineAt{lineIdx + 1, at}
		}
	}

	// node id => peer id => cid => timestamp
	nodePeerCidAt := func(typ string) map[string]map[string]map[string]*lineAt {
		res := make(map[string]map[string]map[string]*lineAt)
		for lineIdx, log := range logs {
			if !strings.Contains(log["msg"].(string), "sent message") {
				continue
			}
			if _, ok := log["type"]; !ok {
				continue
			}
			if !strings.HasPrefix(log["type"].(string), typ) {
				continue
			}

			localNode := log["local"].(string)
			at := log["ts"].(time.Time)
			c := log["cid"].(string)
			to := log["to"].(string)

			byNode, ok := res[localNode]
			if !ok {
				byNode = make(map[string]map[string]*lineAt)
				res[localNode] = byNode
			}

			byPeer, ok := byNode[to]
			if !ok {
				byPeer = make(map[string]*lineAt)
				byNode[to] = byPeer
			}
			byPeer[c] = &lineAt{lineIdx + 1, at}
		}
		return res
	}

	// node id => peer id => cid => timestamp
	sentWants := nodePeerCidAt("WANT")
	sentCancels := nodePeerCidAt("CANCEL")

	// Check if a want was sent to any peer after receiving the corresponding block
	for localNode, blkAt := range rcvdBlocks {
		for blk, rcvdAt := range blkAt {
			for wantTo, peerWants := range sentWants[localNode] {
				if wantAt, ok := peerWants[blk]; ok {
					// it's possible a block will be received right as a want is
					// being sent, so allow a little bit of processing time
					processingTime := 30 * time.Millisecond
					if wantAt.at.After(rcvdAt.at.Add(processingTime)) {
						msg := "Line %d: %s -> %s want %s: should not send want after receiving block "
						msg += "(Line %d: rcv %s)"
						return fmt.Errorf(msg, wantAt.line, localNode, wantTo, blk, rcvdAt.line, blk)
					}
				}
			}
		}
	}

	// Check that when a block was received, cancel was sent to each peer to
	// whom the want was sent
	for localNode, blkAt := range rcvdBlocks {
		for blk, rcvdAt := range blkAt {
			for wantTo, peerWants := range sentWants[localNode] {
				if _, ok := peerWants[blk]; ok {
					if _, ok := sentCancels[localNode][wantTo][blk]; !ok {
						return fmt.Errorf("Cancel was not sent to %s after receiving block %s (Line %d)", wantTo, blk, rcvdAt.line)
					}
				}
			}
		}
	}

	return nil
}

func checkWantSentOnce(logs []map[string]interface{}) error {
	// node id => peer id => cid => want type => timestamp => line number
	wants := make(map[string]map[string]map[string]map[string]map[time.Time]int)
	for lineIdx, log := range logs {
		if !strings.Contains(log["msg"].(string), "sent message") {
			continue
		}
		if _, ok := log["type"]; !ok {
			continue
		}
		if !strings.HasPrefix(log["type"].(string), "WANT") {
			continue
		}

		localNode := log["local"].(string)
		at := log["ts"].(time.Time)
		c := log["cid"].(string)
		to := log["to"].(string)
		typ := log["type"].(string)

		byNode, ok := wants[localNode]
		if !ok {
			byNode = make(map[string]map[string]map[string]map[time.Time]int)
			wants[localNode] = byNode
		}

		byPeer, ok := byNode[to]
		if !ok {
			byPeer = make(map[string]map[string]map[time.Time]int)
			byNode[to] = byPeer
		}

		byCid, ok := byPeer[c]
		if !ok {
			byCid = make(map[string]map[time.Time]int)
			byPeer[c] = byCid
		}

		byWantType, ok := byCid[typ]
		if !ok {
			byWantType = make(map[time.Time]int)
			byCid[typ] = byWantType
		}

		byWantType[at] = lineIdx + 1
	}

	// node id => peer id => cid => timestamp => line number
	cancels := make(map[string]map[string]map[string]map[time.Time]int)
	for lineIdx, log := range logs {
		if !strings.Contains(log["msg"].(string), "sent message") {
			continue
		}
		if _, ok := log["type"]; !ok {
			continue
		}
		if !strings.HasPrefix(log["type"].(string), "CANCEL") {
			continue
		}

		localNode := log["local"].(string)
		at := log["ts"].(time.Time)
		c := log["cid"].(string)
		to := log["to"].(string)

		byNode, ok := cancels[localNode]
		if !ok {
			byNode = make(map[string]map[string]map[time.Time]int)
			cancels[localNode] = byNode
		}

		byPeer, ok := byNode[to]
		if !ok {
			byPeer = make(map[string]map[time.Time]int)
			byNode[to] = byPeer
		}

		byCid, ok := byPeer[c]
		if !ok {
			byCid = make(map[time.Time]int)
			byPeer[c] = byCid
		}

		byCid[at] = lineIdx + 1
	}

	// node id => cid => timestamp => line number
	broadcasts := make(map[string]map[string]map[time.Time]int)
	for lineIdx, log := range logs {
		if !strings.Contains(log["msg"].(string), "broadcastWantHaves") {
			continue
		}

		localNode := log["local"].(string)
		at := log["ts"].(time.Time)
		cids := log["cids"].([]interface{})

		byNode, ok := broadcasts[localNode]
		if !ok {
			byNode = make(map[string]map[time.Time]int)
			broadcasts[localNode] = byNode
		}

		for _, cenvi := range cids {
			cenv := cenvi.(map[string]interface{})
			c := cenv["/"].(string)

			byCid, ok := byNode[c]
			if !ok {
				byCid = make(map[time.Time]int)
				byNode[c] = byCid
			}

			byCid[at] = lineIdx + 1
		}
	}

	// Check that want-have was only sent once from a node to each of its peers
	// Check that want-block was only sent once from a node to each of its peers
	for localNode, byNode := range wants {
		for sentTo, byPeer := range byNode {
			for c, byCid := range byPeer {
			checkCids:
				for wantType, byWantType := range byCid {
					if len(byWantType) < 2 {
						continue
					}

					var times []time.Time
					for t := range byWantType {
						times = append(times, t)
					}
					sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
					// fmt.Printf("times %s\n", times)

					// It's ok to send two wants as long as there's a cancel between them
					last := len(times) - 1
					wantTime := times[last]
					previousTime := times[last-1]
					for cancelAt := range cancels[localNode][sentTo][c] {
						// fmt.Printf("cancel %s\n", cancelAt)
						if cancelAt.After(previousTime) && cancelAt.Before(wantTime) {
							break checkCids
						}
					}

					// This can also occur if a block is received right at the
					// same time as a broadcast is triggered, and the want is
					// added to the MessageQueue just after the cancel (before
					// the cancel has actually been sent)
					gracePeriod := 30 * time.Millisecond
					for t := range broadcasts[localNode][c] {
						// fmt.Printf("wantTime: %s - time: %s = %s\n", wantTime, t, wantTime.Sub(t))
						if t.Before(wantTime) && wantTime.Sub(t) < gracePeriod {
							break checkCids
						}
					}

					msg := "Line %d: %s -> %s %s %s - %s was sent more than once to the same peer "
					msg += "(Previously sent %s %s to %s on Line %d)"
					firstLine := byWantType[times[0]]
					secondLine := byWantType[times[1]]
					return fmt.Errorf(msg, secondLine, localNode, sentTo, wantType, c, wantType, wantType, c, sentTo, firstLine)
				}
			}
		}
	}

	return nil
}

func checkTimeoutAfterBlockReceived(logs []map[string]interface{}) error {
	// node id => cid => timestamp
	rcvdBlocks := make(map[string]map[string]*lineAt)
	for lineIdx, log := range logs {
		if !strings.Contains(log["msg"].(string), "Bitswap <- block") {
			continue
		}

		pid := log["local"].(string)
		at := log["ts"].(time.Time)
		c := log["cid"].(string)

		peerBlks, ok := rcvdBlocks[pid]
		if !ok {
			peerBlks = make(map[string]*lineAt)
			rcvdBlocks[pid] = peerBlks
		}

		blkAt, ok := peerBlks[c]
		if !ok || at.Before(blkAt.at) {
			peerBlks[c] = &lineAt{lineIdx + 1, at}
		}
	}

	// node id => peer id => cid => timestamp
	timeouts := make(map[string]map[string]map[string]*lineAt)

	for lineIdx, log := range logs {
		if !strings.Contains(log["msg"].(string), "timeout waiting for blocks") {
			continue
		}

		localNode := log["local"].(string)
		at := log["ts"].(time.Time)
		cids := log["cids"].([]interface{})
		to := log["peer"].(string)

		byNode, ok := timeouts[localNode]
		if !ok {
			byNode = make(map[string]map[string]*lineAt)
			timeouts[localNode] = byNode
		}

		byPeer, ok := byNode[to]
		if !ok {
			byPeer = make(map[string]*lineAt)
			byNode[to] = byPeer
		}

		for _, cenvi := range cids {
			cenv := cenvi.(map[string]interface{})
			c := cenv["/"].(string)
			tAt, ok := byPeer[c]
			if !ok || at.Before(tAt.at) {
				byPeer[c] = &lineAt{lineIdx + 1, at}
			}
		}
	}

	// Check each timeout to make sure it doesn't come after receiving
	// the corresponding block
	for localNode, byNode := range timeouts {
		for timeoutFor, byPeer := range byNode {
			for c, timeoutAt := range byPeer {
				if rcvdAt, ok := rcvdBlocks[localNode][c]; ok {
					// it's possible a block will be received right as a timeout
					// occurs, so allow a little bit of processing time
					processingTime := 30 * time.Millisecond
					if timeoutAt.at.After(rcvdAt.at.Add(processingTime)) {
						msg := "Line %d: %s -> %s processed want-block timeout at %s but block was already received at %s "
						msg += "(Line %d)"
						return fmt.Errorf(msg, timeoutAt.line, localNode, timeoutFor, timeoutAt.at, rcvdAt.line)
					}
				}
			}
		}
	}

	return nil
}
