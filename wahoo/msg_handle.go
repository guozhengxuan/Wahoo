package wahoo

import (
	"github.com/gitzhang10/BFT/common"
)

func (n *Node) HandleMsgLoop() {
	msgCh := n.trans.MsgChan()
	for {
		select {
		case msgWithSig := <-msgCh:
			switch msgAsserted := msgWithSig.Msg.(type) {
			case Block:
				if !n.verifySigED25519(msgAsserted.Sender, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the block's signature", "round", msgAsserted.Round,
						"sender", msgAsserted.Sender)
					continue
				}
				if msgAsserted.Round%2 == 0 {
					go n.pb.HandleBlockMsg(&msgAsserted)
				} else {
					go n.handleFastBlockMsg(&msgAsserted)
				}
			case Elect:
				if !n.verifySigED25519(msgAsserted.Sender, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the echo's signature", "round", msgAsserted.Round,
						"sender", msgAsserted.Sender)
					continue
				}
				go n.handleElectMsg(&msgAsserted)
			case Ready:
				if !n.verifySigED25519(msgAsserted.ReadySender, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the ready's signature", "round", msgAsserted.Round,
						"sender", msgAsserted.ReadySender, "blockSender", msgAsserted.BlockSender)
					continue
				}
				go n.handleReadyMsg(&msgAsserted)
			case Done:
				if !n.verifySigED25519(msgAsserted.DoneSender, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the done's signature", "round", msgAsserted.Round,
						"sender", msgAsserted.DoneSender, "blockSender", msgAsserted.BlockSender)
					continue
				}
				go n.handleDoneMsg(&msgAsserted)
			case Vote:
				if !n.verifySigED25519(msgAsserted.VoteSender, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the vote's signature", "round", msgAsserted.Round,
						"sender", msgAsserted.VoteSender, "blockSender", msgAsserted.BlockSender)
					continue
				}
				go n.pb.HandleVoteMsg(&msgAsserted)
			case ReVote:
				n.logger.Debug("Revote is received by", "node", n.name, "round",
					msgAsserted.Round, "proposer", msgAsserted.ReVoteSender)
				//TO DO...
			}
		}
	}
}

func (n *Node) handlePBBlock(block *Block) {
	go n.tryToUpdateDAG(block)
}

func (n *Node) handleFastBlockMsg(block *Block) {
	hash, _ := common.GetHash(block)
	// this is a simple way, need modify...
	go n.tryToUpdateDAG(block)
	n.lock.Lock()
	if !n.blockSend[block.Round+1] {
		n.lock.Unlock()
		n.sendReady(block.Round, hash, block.Sender)
	} else {
		n.lock.Unlock()
	}
}

func (n *Node) handleReadyMsg(ready *Ready) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.storeReady(ready)
	go n.checkIfEnoughReady(ready)
}

func (n *Node) handleElectMsg(elect *Elect) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.storeElectMsg(elect)
	n.tryToElectLeader(elect.Round)
}

func (n *Node) handleDoneMsg(done *Done) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.logger.Debug("Done is received by", "node", n.name, "round",
		done.Round, "proposer", done.DoneSender)
	n.storeDone(done)
	go n.tryToNextRound(done.Round)
	n.tryToCommitLeader(done.Round)
}

func (n *Node) PBOutputBlockLoop() {
	dataCh := n.pb.ReturnBlockChan()
	for {
		select {
		case block := <-dataCh:
			n.logger.Debug("Block is received by from PB", "node", n.name, "round",
				block.Round, "proposer", block.Sender)
			go n.handlePBBlock(&block)
		}
	}
}

// func (n *Node) DoneOutputLoop() {
// 	dataCh := n.pb.ReturnDoneChan()
// 	for {
// 		select {
// 		case done := <-dataCh:
// 			go n.handleDoneMsg(&done)
// 			// sender broadcast done to all nodes
// 			go n.broadcastDone(done)
// 		}
// 	}
// }
