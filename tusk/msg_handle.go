package tusk

import (
	"strconv"

	"github.com/gitzhang10/BFT/common"
	"github.com/gitzhang10/BFT/rbc"
)

// HandleMsgLoop starts a loop to deal with the msgs from other peers.
func (n *Node) HandleMsgLoop() {
	msgCh := n.trans.MsgChan()
	for {
		select {
		case msgWithSig := <-msgCh:
			if n.isFaulty {
				continue
			}
			switch msgAsserted := msgWithSig.Msg.(type) {
			case rbc.VALMsg:
				if !n.verifySigED25519(msgAsserted.Proposer, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the VALMsg proposer's signature", "round", msgAsserted.DataSN,
						"proposer", msgAsserted.Proposer)
					continue
				}
				go n.rbc.HandleRBCValMsg(n.privateKey, &msgAsserted)
			case rbc.ECHOMsg:
				if !n.verifySigED25519(msgAsserted.Sender, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the ECHOMsg sender's signature", "round", msgAsserted.DataSN,
						"proposer", msgAsserted.Proposer, "sender", msgAsserted.Sender)
					continue
				}
				addr := n.clusterAddr[msgAsserted.Sender]
				port := strconv.Itoa(n.clusterPort[msgAsserted.Sender])
				index, _ := n.clusterAddrWithPorts[addr+":"+port]
				go n.rbc.HandleRBCEchoMsg(n.privateKey, int(index), &msgAsserted)
			case rbc.READYMsg:
				if !n.verifySigED25519(msgAsserted.Sender, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the READYMsg sender's signature", "round", msgAsserted.DataSN,
						"proposer", msgAsserted.Proposer, "sender", msgAsserted.Sender)
					continue
				}
				go n.rbc.HandleRBCReadyMsg(n.privateKey, &msgAsserted)
			case Elect:
				if !n.verifySigED25519(msgAsserted.Sender, msgWithSig.Msg, msgWithSig.Sig) {
					n.logger.Error("fail to verify the Elect's signature", "round", msgAsserted.Round,
						"sender", msgAsserted.Sender, "blockSender")
					continue
				}
				go n.handleElectMsg(&msgAsserted)
			}
		}
	}
}

func (n *Node) handleElectMsg(elect *Elect) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.storeElectMsg(elect)
	n.tryToElectLeader(elect.Round)
}

func (n *Node) handleNewBlockMsg(block *Block) {
	go n.tryToUpdateDAG(block)
}

func (n *Node) ConstructedBlockLoop() {
	dataCh := n.rbc.ReturnDataChan()
	for {
		select {
		case data := <-dataCh:
			block := new(Block)
			if err := common.Decode(data, block); err != nil {
				n.logger.Debug("Data received is not a block")
			} else {
				n.logger.Debug("Block is received by from RBC", "node", n.name, "round",
					block.Round, "proposer", block.Sender)
				go n.handleNewBlockMsg(block)
			}
		}
	}
}
