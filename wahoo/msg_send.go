package wahoo

import (
	"strconv"

	"github.com/gitzhang10/BFT/common"
	"github.com/gitzhang10/BFT/conn"
	"github.com/gitzhang10/BFT/sign"
)

func (n *Node) broadcastBlock(round uint64) {
	previousHash := n.selectPreviousBlocks(round - 1)
	block := n.NewBlock(round, previousHash)
	n.pb.BroadcastBlock(block)
	n.lock.Lock()
	n.blockSend[round] = true
	n.lock.Unlock()
}

// func (n *Node) sendReady(round uint64, hash []byte, blockSender string) {
// 	partialSig := sign.SignTSPartial(n.tsPrivateKey, hash)
// 	ready := Ready{
// 		ReadySender: n.name,
// 		BlockSender: blockSender,
// 		Round:       round,
// 		Hash:        hash,
// 		PartialSig:  partialSig,
// 	}
// 	err := n.send(ReadyTag, ready, blockSender)
// 	if err != nil {
// 		panic(err)
// 	}
// }

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

func (n *Node) broadcastDone(done Done) {
	// var doneQc [][]byte
	// qc := sign.AssembleIntactTSPartial(done.Done, n.tsPublicKey, done.Hash, n.quorumNum, n.nodeNum)
	// doneQc = append(doneQc, qc)

	err := n.broadcast(DoneTag, done)
	if err != nil {
		panic(err)
	}
}

func (n *Node) broadcastReVote(round uint64, voted bool) {
	revote := ReVote{
		ReVoteSender: n.name,
		BlockSender:  n.leader[round],
		Round:        round,
		Voted:        voted,
		Hash:         nil, // TO DO...
	}
	err := n.broadcast(ReVoteTag, revote)
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

//only send message to one node
func (n *Node) send(msgType uint8, msg interface{}, target string) error {
	msgAsBytes, err := common.Encode(msg)
	if err != nil {
		return err
	}
	sig := sign.SignEd25519(n.privateKey, msgAsBytes)
	addr := n.clusterAddr[target]
	port := n.clusterPort[target]
	addWithPort := addr + ":" + strconv.Itoa(port)
	netConn, err := n.trans.GetConn(addWithPort)
	if err != nil {
		return err
	}
	if err = conn.SendMsg(netConn, msgType, msg, sig); err != nil {
		return err
	}
	if err = n.trans.ReturnConn(netConn); err != nil {
		return err
	}
	return nil
}
