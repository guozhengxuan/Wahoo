package wahoo

import (
	"crypto/ed25519"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/gitzhang10/BFT/config"
	"github.com/gitzhang10/BFT/sign"
)

var clusterAddr = map[string]string{
	"node0": "127.0.0.1",
	"node1": "127.0.0.1",
	"node2": "127.0.0.1",
	"node3": "127.0.0.1",
	"node4": "127.0.0.1",
	"node5": "127.0.0.1",
	"node6": "127.0.0.1",
}
var clusterPort = map[string]int{
	"node0": 8000,
	"node1": 8010,
	"node2": 8020,
	"node3": 8030,
	"node4": 8040,
	"node5": 8050,
	"node6": 8060,
}

func setupNodes(logLevel int, batchSize int, round int) []*Node {
	names := make([]string, 7)
	clusterAddrWithPorts := make(map[string]uint8)
	for name, addr := range clusterAddr {
		rn := []rune(name)
		i, _ := strconv.Atoi(string(rn[4:]))
		names[i] = name
		clusterAddrWithPorts[addr+":"+strconv.Itoa(clusterPort[name])] = uint8(i)
	}

	// create the ED25519 keys
	privKeys := make([]ed25519.PrivateKey, 7)
	pubKeys := make([]ed25519.PublicKey, 7)
	for i := 0; i < 7; i++ {
		privKeys[i], pubKeys[i] = sign.GenED25519Keys()
	}
	pubKeyMap := make(map[string]ed25519.PublicKey)
	for i := 0; i < 7; i++ {
		pubKeyMap[names[i]] = pubKeys[i]
	}

	// create the threshold keys
	shares, pubPoly := sign.GenTSKeys(3, 7)

	// create configs and nodes
	confs := make([]*config.Config, 7)
	nodes := make([]*Node, 7)
	for i := 0; i < 7; i++ {
		confs[i] = config.New(names[i], 10, clusterAddr, clusterPort, nil, clusterAddrWithPorts, nil, pubKeyMap, privKeys[i], pubPoly, shares[i], logLevel, false, batchSize, round)
		nodes[i] = NewNode(confs[i])
		if err := nodes[i].StartP2PListen(); err != nil {
			panic(err)
		}
		nodes[i].InitPB(confs[i])
	}
	for i := 0; i < 7; i++ {
		go nodes[i].EstablishP2PConns()
	}
	time.Sleep(time.Second)
	return nodes
}

func clean(nodes []*Node) {
	for _, n := range nodes {
		n.trans.GetStreamContext().Done()
		_ = n.trans.Close()
	}
}

func TestWith7Nodes(t *testing.T) {
	nodes := setupNodes(3, 50, 500)
	for i := 0; i < 7; i++ {
		fmt.Printf("node%d starts the Wahoo!\n", i)
		go nodes[i].RunLoop()
		go nodes[i].HandleMsgLoop()
		go nodes[i].PBOutputBlockLoop()
	}

	// wait all nodes finish
	time.Sleep(25 * time.Second)

	clean(nodes)
}
