package tusk

import (
	"github.com/gitzhang10/BFT/common"
	"github.com/gitzhang10/BFT/conn"
	"github.com/gitzhang10/BFT/sign"
)

// we use RBC to broadcast a block
func (n *Node) broadcastBlock(round uint64) {
	previousHash := n.selectPreviousBlocks(round-1)
	block := n.newBlock(round, previousHash)
	blockAsBytes, err := common.Encode(block)
	if err != nil {
		panic(err)
	}
	if err := n.rbc.BroadcastVALMsg(n.privateKey, round, blockAsBytes); err != nil {
		panic(err)
	}
}

func (n *Node) broadcastElect(round uint64) {
	data, err := common.Encode(round)
	if err != nil {
		panic(err)
	}
	partialSig := sign.SignTSPartial(n.tsPrivateKey, data)
	elect := Elect{
		Sender:     n.name,
		Round:      round,
		PartialSig: partialSig,
	}
	err = n.broadcast(ElectTag, elect)
	if err != nil {
		panic(err)
	}
}

// send message to all nodes
func (n *Node) broadcast(msgType uint8, msg interface{}) error {
	msgAsBytes, err := common.Encode(msg)
	if err != nil {
		return err
	}
	sig := sign.SignEd25519(n.privateKey, msgAsBytes)
	for addrWithPort := range n.clusterAddrWithPorts {
		netConn, err := n.trans.GetConn(addrWithPort)
		if err != nil {
			return err
		}
		if err = conn.SendMsg(netConn, msgType, msg, sig); err != nil {
			return err
		}

		if err = n.trans.ReturnConn(netConn); err != nil {
			return err
		}
	}
	return nil
}
