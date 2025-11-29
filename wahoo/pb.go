package wahoo

import (
	"crypto/ed25519"
	"strconv"
	"sync"

	"github.com/gitzhang10/BFT/common"
	"github.com/gitzhang10/BFT/conn"
	"github.com/gitzhang10/BFT/sign"
	"go.dedis.ch/kyber/v3/share"
)

// It's PB.

type PB struct {
	name                 string
	clusterAddr          map[string]string // map from name to address
	clusterPort          map[string]int    // map from name to p2pPort
	clusterAddrWithPorts map[string]uint8
	connPool             *conn.NetworkTransport
	nodeNum              int
	quorumNum            int
	pendingBlocks        map[uint64]map[string]*Block // map from round to sender to block
	pendingBlock2s       map[uint64]map[string]*Block
	pendingVote          map[uint64]map[string]int // map from round to block_sender to vote count
	pendingReady         map[uint64]map[string]map[string][]byte // map from round to block_sender to ready_sender to parSig
	privateKey   ed25519.PrivateKey
	tsPublicKey  *share.PubPoly
	tsPrivateKey *share.PriShare
	lock         sync.RWMutex
	blockCh      chan Block
	doneCh               chan Done
	blockOutput map[uint64]map[string]bool // mark whether a block has been output before
	doneOutput           map[uint64]map[string]bool // mark whether a done has been output before
	blockSend            map[uint64]bool            // mark whether have sent block in a round
	block2Send map[uint64]bool
}

func (c *PB) ReturnBlockChan() chan Block {
	return c.blockCh
}

func (c *PB) ReturnDoneChan() chan Done {
	return c.doneCh
}

func NewPBer(name string, clusterAddr map[string]string, clusterPort map[string]int, clusterAddrWithPorts map[string]uint8, connPool *conn.NetworkTransport, q, n int, privateKey ed25519.PrivateKey, tsPublicKey *share.PubPoly, tsPrivateKey *share.PriShare) *PB {
	return &PB{
		name:                 name,
		clusterAddr:          clusterAddr,
		clusterPort:          clusterPort,
		clusterAddrWithPorts: clusterAddrWithPorts,
		connPool:             connPool,
		nodeNum:              n,
		quorumNum:            q,
		pendingBlocks:        make(map[uint64]map[string]*Block),
		pendingBlock2s:       make(map[uint64]map[string]*Block),
		pendingVote:          make(map[uint64]map[string]int),
		pendingReady:         make(map[uint64]map[string]map[string][]byte),
		privateKey: privateKey,
		blockCh:    make(chan Block),
		doneCh:               make(chan Done),
		blockOutput: make(map[uint64]map[string]bool),
		doneOutput:           make(map[uint64]map[string]bool),
		tsPublicKey:  tsPublicKey,
		tsPrivateKey: tsPrivateKey,
		blockSend:            make(map[uint64]bool),
		block2Send: make(map[uint64]bool),
	}
}

func (c *PB) BroadcastBlock(block *Block) {
	err := c.broadcast(ProposalTag, block)
	if err != nil {
		panic(err)
	}
	c.lock.Lock()
	//c.blockSend[block.Round] = true
	c.storeBlockMsg(block)
	c.lock.Unlock()
}

func (c *PB) sendVote(blockSender string, round uint64) {
	vote := Vote{
		VoteSender:  c.name,
		BlockSender: blockSender,
		Round:       round,
	}
	err := c.send(VoteTag, vote, blockSender)
	if err != nil {
		panic(err)
	}
}

//in our design, block2 is emptyblock
func (c *PB) broadcastBlock2(block *Block) {
	err := c.broadcast(ProposalTag, block)
	if err != nil {
		panic(err)
	}
	c.lock.Lock()
	c.block2Send[block.Round] = true
	c.storeBlock2Msg(block)
	c.lock.Unlock()
}

func (c *PB) sendReady(round uint64, hash []byte, blockSender string) {
	partialSig := sign.SignTSPartial(c.tsPrivateKey, hash)
	ready := Ready{
		ReadySender: c.name,
		BlockSender: blockSender,
		Round:       round,
		Hash:        hash,
		PartialSig:  partialSig,
	}
	err := c.send(ReadyTag, ready, blockSender)
	if err != nil {
		panic(err)
	}
}

func (c *PB) HandleBlockMsg(block *Block) {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch block.Tag {
	case 1:
		c.storeBlockMsg(block)
		c.sendVote(block.Sender, block.Round)
		if _, ok := c.pendingBlock2s[block.Round]; !ok {
			c.pendingBlock2s[block.Round] = make(map[string]*Block)
		}
		if _, ok := c.pendingBlock2s[block.Round][block.Sender]; ok {
			go c.tryToOutputBlocks(block.Round, block.Sender)
		}
	case 2:
		c.storeBlock2Msg(block)

		if block.Round%2 == 1 {
			hash, _ := common.GetHash(block)
			c.sendReady(block.Round, hash, block.Sender)
		}
		
		go c.tryToOutputBlocks(block.Round, block.Sender)
	}
}

func (c *PB) HandleVoteMsg(vote *Vote) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.storeVoteMsg(vote)
	go c.checkIfQuorumVote(vote.Round, vote.BlockSender)
}

func (c *PB) handleReadyMsg(ready *Ready) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.storeReadyMsg(ready)
	go c.checkIfQuorumReady(ready)
}

func (c *PB) storeBlockMsg(block *Block) {
	if _, ok := c.pendingBlocks[block.Round]; !ok {
		c.pendingBlocks[block.Round] = make(map[string]*Block)
	}
	c.pendingBlocks[block.Round][block.Sender] = block
}

func (c *PB) storeBlock2Msg(block *Block) {
	if _, ok := c.pendingBlock2s[block.Round]; !ok {
		c.pendingBlock2s[block.Round] = make(map[string]*Block)
	}
	c.pendingBlock2s[block.Round][block.Sender] = block
}

func (c *PB) storeVoteMsg(vote *Vote) {
	if _, ok := c.pendingVote[vote.Round]; !ok {
		c.pendingVote[vote.Round] = make(map[string]int)
	}
	c.pendingVote[vote.Round][vote.BlockSender]++
}

func (c *PB) storeReadyMsg(ready *Ready) {
	if _, ok := c.pendingReady[ready.Round]; !ok {
		c.pendingReady[ready.Round] = make(map[string]map[string][]byte)
	}
	if _, ok := c.pendingReady[ready.Round][ready.BlockSender]; !ok {
		c.pendingReady[ready.Round][ready.BlockSender] = make(map[string][]byte)
	}
	c.pendingReady[ready.Round][ready.BlockSender][ready.ReadySender] = ready.PartialSig
}

func (c *PB) checkIfQuorumVote(round uint64, blockSender string) {
	c.lock.Lock()
	voteCount := c.pendingVote[round][blockSender]
	if voteCount >= c.quorumNum {
		if !c.block2Send[round] {
			c.lock.Unlock()
			block2 := c.generateBlock2(round, blockSender)
			c.broadcastBlock2(block2)
		} else {
			c.lock.Unlock()
		}
		go c.tryToOutputBlocks(round, blockSender)
	} else {
		c.lock.Unlock()
	}
}

func (c *PB) checkIfQuorumReady(ready *Ready) {
	c.lock.Lock()
	readies := c.pendingReady[ready.Round][ready.BlockSender]
	if _, ok := c.doneOutput[ready.Round]; !ok {
		c.doneOutput[ready.Round] = make(map[string]bool)
	}
	if len(readies) >= c.quorumNum && !c.doneOutput[ready.Round][ready.BlockSender] {
		c.doneOutput[ready.Round][ready.BlockSender] = true
		var partialSig [][]byte
		for _, parSig := range readies {
			partialSig = append(partialSig, parSig)
		}
		c.lock.Unlock()
		// done := sign.AssembleIntactTSPartial(partialSig, c.tsPublicKey, ready.Hash, c.quorumNum, c.nodeNum)
		doneMsg := &Done{
			DoneSender:  c.name,
			BlockSender: ready.BlockSender,
			Done:        partialSig,
			Hash:        ready.Hash,
			Round:       ready.Round,
		}
		c.doneCh <- *doneMsg
	} else {
		c.lock.Unlock()
	}

}

func (c *PB) tryToOutputBlocks(round uint64, sender string) {
	c.lock.Lock()
	if _, ok := c.blockOutput[round]; !ok {
		c.blockOutput[round] = make(map[string]bool)
	}
	if c.blockOutput[round][sender] {
		c.lock.Unlock()
		return
	}

	if _, ok := c.pendingBlocks[round]; !ok {
		c.pendingBlocks[round] = make(map[string]*Block)
	}

	if _, ok := c.pendingBlocks[round][sender]; !ok {
		c.lock.Unlock()
		return
	}
	block := c.pendingBlocks[round][sender]
	c.blockOutput[round][sender] = true
	c.lock.Unlock()
	c.blockCh <- *block
}

//an empty block
func (c *PB) generateBlock2(round uint64, blockSender string) *Block {
	return &Block{
		Sender:       blockSender,
		Round:        round,
		PreviousHash: make(map[string][]byte),
		Txs:          nil,
		Tag:          2,
	}
}

// send message to all nodes
func (c *PB) broadcast(msgType uint8, msg interface{}) error {
	msgAsBytes, err := common.Encode(msg)
	if err != nil {
		return err
	}
	sig := sign.SignEd25519(c.privateKey, msgAsBytes)
	for addrWithPort := range c.clusterAddrWithPorts {
		netConn, err := c.connPool.GetConn(addrWithPort)
		if err != nil {
			return err
		}
		if err = conn.SendMsg(netConn, msgType, msg, sig); err != nil {
			return err
		}

		if err = c.connPool.ReturnConn(netConn); err != nil {
			return err
		}
	}
	return nil
}

//only send message to one node
func (c *PB) send(msgType uint8, msg interface{}, target string) error {
	msgAsBytes, err := common.Encode(msg)
	if err != nil {
		return err
	}
	sig := sign.SignEd25519(c.privateKey, msgAsBytes)
	addr := c.clusterAddr[target]
	port := c.clusterPort[target]
	addWithPort := addr + ":" + strconv.Itoa(port)
	netConn, err := c.connPool.GetConn(addWithPort)
	if err != nil {
		return err
	}
	if err = conn.SendMsg(netConn, msgType, msg, sig); err != nil {
		return err
	}
	if err = c.connPool.ReturnConn(netConn); err != nil {
		return err
	}
	return nil
}
