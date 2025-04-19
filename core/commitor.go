package core

import (
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/store"
	"sync"
)

type pattern int

const (
	toCommit pattern = iota
	toskip
	undecide
	unjudge
)

type LocalDAG struct {
	store        *store.Store
	committee    Committee
	muBlock      *sync.RWMutex
	blockDigests map[crypto.Digest]NodeID // store hash of block that has received
	muDAG        *sync.RWMutex
	localDAG     map[int]map[NodeID][]crypto.Digest // local DAG
	edgesDAG     map[int]map[NodeID][]map[crypto.Digest]NodeID
	iscertDAG    map[int]map[NodeID][]pattern //proposer pattern

}

func NewLocalDAG(store *store.Store, committee Committee) *LocalDAG {
	return &LocalDAG{
		muBlock:      &sync.RWMutex{},
		muDAG:        &sync.RWMutex{},
		blockDigests: make(map[crypto.Digest]NodeID),
		localDAG:     make(map[int]map[NodeID][]crypto.Digest),
		edgesDAG:     make(map[int]map[NodeID][]map[crypto.Digest]NodeID),
		iscertDAG:    make(map[int]map[NodeID][]pattern),
		store:        store,
		committee:    committee,
	}
}

// IsReceived: digests is received ?
func (local *LocalDAG) IsReceived(digests ...crypto.Digest) (bool, []crypto.Digest) {
	local.muBlock.RLock()
	defer local.muBlock.RUnlock()

	var miss []crypto.Digest
	var flag bool = true
	for _, d := range digests {
		if _, ok := local.blockDigests[d]; !ok {
			miss = append(miss, d)
			flag = false
		}
	}

	return flag, miss
}

func (local *LocalDAG) ReceiveBlock(round int, node NodeID, digest crypto.Digest, references map[crypto.Digest]NodeID) {
	local.muBlock.Lock()
	local.blockDigests[digest] = node
	local.muBlock.Unlock()

	local.muDAG.Lock()
	vslot, ok := local.localDAG[round]
	eslot := local.edgesDAG[round]
	certslot := local.iscertDAG[round]
	if !ok {
		vslot = make(map[NodeID][]crypto.Digest)
		eslot = make(map[NodeID][]map[crypto.Digest]NodeID)
		certslot = make(map[NodeID][]pattern)
		local.localDAG[round] = vslot
		local.edgesDAG[round] = eslot
		local.iscertDAG[round] = certslot
	}
	vslot[node] = append(vslot[node], digest)
	eslot[node] = append(eslot[node], references)
	certslot[node] = append(certslot[node], unjudge)
	local.muDAG.Unlock()
}

func (local *LocalDAG) GetRoundReceivedBlockNums(round int) (nums int) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()

	nums = len(local.localDAG[round])

	return
}

// func (local *LocalDAG) GetReceivedBlock(round int, node NodeID) (crypto.Digest, bool) {
// 	local.muDAG.RLock()
// 	defer local.muDAG.RUnlock()
// 	if slot, ok := local.localDAG[round]; ok {
// 		d, ok := slot[node]
// 		return d, ok
// 	}
// 	return crypto.Digest{}, false
// }

func (local *LocalDAG) GetReceivedBlockReference(round int, node NodeID) (map[crypto.Digest]NodeID, bool) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()
	if slot, ok := local.edgesDAG[round]; ok {
		reference, ok := slot[node]
		return reference, ok
	}
	return nil, false
}

func (local *LocalDAG) GetRoundReceivedBlock(round int) (digests map[crypto.Digest]NodeID) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()
	digests = make(map[crypto.Digest]NodeID)
	for id, d := range local.localDAG[round] {
		digests[d] = id
	}

	return digests
}

// judge if bvote refer to bproposer
func (local *LocalDAG) isVote(Bproposer *Block, Bvote *Block) bool {
	id := Bproposer.Author
	r := Bproposer.Round

	return local.supportedblock(Bvote, id, r) == Bproposer.Hash()

	// if hash, ok := local.supportedblock(Bvote, id, r); !ok{
	// 	return ok
	// }else{
	// 	if hash != Bproposer.Hash(){
	// 		return false
	// 	}
	// }
	// return true
}

func (local *LocalDAG) supportedblock(b *Block, id NodeID, r int) crypto.Digest {
	local.muDAG.RLock()
	digests := local.edgesDAG[b.Round][b.Author]
	local.muDAG.RUnlock()

	if r >= b.Round {
		return crypto.Digest{}
	}
	for key, value := range digests {
		if value == id && r == b.Round-1 {
			return key
		}

	}
	return crypto.Digest{}
}

func (local *LocalDAG) isCert(Bproposer *Block, Bcert *Block) bool {
	local.muDAG.RLock()
	digests := local.edgesDAG[Bcert.Round][Bcert.Author]
	local.muDAG.RUnlock()

	var voteCount int = 0
	for key := range digests {
		if bvote, err := getBlock(local.store, key); err != nil {
			logger.Warn.Println(err)
			continue
		} else {
			if local.isVote(Bproposer, bvote) {
				voteCount++
			}
		}
	}

	return voteCount >= local.committee.HightThreshold()

}

func (local *LocalDAG) GetVotingBlocks(round int) map[crypto.Digest]NodeID {
	nums := local.GetRoundReceivedBlockNums(round)

	if nums >= local.committee.HightThreshold() {
		return local.GetRoundReceivedBlock(round)
	}

	return nil
}

func (local *LocalDAG) GetDecisonBlocks(b *Block) map[crypto.Digest]NodeID {

	return local.GetRoundReceivedBlock(b.Round + 2)

}

func (local *LocalDAG) skippedProposer(id NodeID, round int) bool {
	var res int = 0

	digests := local.GetVotingBlocks(round + 1)
	for digest := range digests {
		if block, err := getBlock(local.store, digest); err != nil {
			logger.Warn.Println(err)
			continue
		} else {
			if !containsValue(block.Reference, id) {
				res++
			}
		}
	}
	return res >= local.committee.HightThreshold()
}

func containsValue(m map[crypto.Digest]NodeID, target NodeID) bool {
	for _, v := range m {
		if v == target {
			return true
		}
	}
	return false
}

func (local *LocalDAG) committedProposer(b *Block) bool {
	var certCount int = 0

	digests := local.GetDecisonBlocks(b)
	for digest := range digests {
		if block, err := getBlock(local.store, digest); err != nil {
			logger.Warn.Println(err)
			continue
		} else {
			if local.isCert(b, block) {
				certCount++
			}
		}
	}

	return certCount >= local.committee.HightThreshold()
}

// type ABAid struct {
// 	round int
// 	node  NodeID
// }

type Commitor struct {
	commitChannel chan<- *Block
	localDAG      *LocalDAG
	commitBlocks  map[crypto.Digest]struct{}
	notify        chan struct{}
	inner         chan crypto.Digest
	store         *store.Store
	// startABA       chan<- *ABAid
	undecidedround int
	undecidednode  NodeID
}

func NewCommitor(localDAG *LocalDAG, store *store.Store, commitChannel chan<- *Block) *Commitor {
	c := &Commitor{
		localDAG:      localDAG,
		commitChannel: commitChannel,
		commitBlocks:  make(map[crypto.Digest]struct{}),
		notify:        make(chan struct{}, 100),
		store:         store,
		inner:         make(chan crypto.Digest),
		//startABA:       startABA,
		undecidedround: 0,
		undecidednode:  0,
	}
	go c.run()
	return c
}

func (c *Commitor) run() {

	go func() {
		for digest := range c.inner {
			if block, err := getBlock(c.store, digest); err != nil {
				logger.Warn.Println(err)
			} else {
				if block.Batch.Txs != nil {
					//BenchMark Log
					logger.Info.Printf("commit Block round %d node %d batch_id %d \n", block.Round, block.Author, block.Batch.ID)
				}
				c.commitChannel <- block
			}
		}
	}()

	for range c.notify {
		c.tryDerictdecide()
	}

}

func (c *Commitor) tryDerictdecide() {

	for {
		logger.Debug.Printf("procesing try decide round %d node %d \n", c.undecidedround, c.undecidednode)

		if c.localDAG.GetRoundReceivedBlockNums(c.undecidedround+1) >= c.localDAG.committee.HightThreshold() {

			if c.localDAG.skippedProposer(c.undecidednode, c.undecidedround) {
				logger.Debug.Printf(" round %d node %d is skipped \n", c.undecidedround, c.undecidednode)
				c.localDAG.iscertDAG[c.undecidedround][c.undecidednode] = toskip

				c.advancedpointer()

				// if c.ifLossLiveness() {

				// }
				continue
			} else if c.localDAG.GetRoundReceivedBlockNums(c.undecidedround+2) >= c.localDAG.committee.HightThreshold() {
				c.localDAG.muDAG.Lock()
				digest := c.localDAG.localDAG[c.undecidedround][c.undecidednode]
				c.localDAG.muDAG.Unlock()

				if block, err := getBlock(c.store, digest); err != nil {
					logger.Warn.Println(err)
				} else if c.localDAG.committedProposer(block) {
					logger.Debug.Printf(" round %d node %d is to commit \n", c.undecidedround, c.undecidednode)
					c.localDAG.iscertDAG[c.undecidedround][c.undecidednode] = toCommit
					c.inner <- digest

					c.advancedpointer()
					continue
				} else {
					c.localDAG.iscertDAG[c.undecidedround][c.undecidednode] = undecide
					logger.Debug.Printf(" round %d node %d is undecided \n", c.undecidedround, c.undecidednode)
					c.advancedpointer()
					//START AN ABA
					break
				}
			} else {
				logger.Debug.Printf("cert not finish cant decide round %d node %d \n", c.undecidedround, c.undecidednode)
				break
			}
		} else {
			logger.Debug.Printf("vote not finish cant decide round %d node %d \n", c.undecidedround, c.undecidednode)
			break
		}
	}

}

func (c *Commitor) advancedpointer() {
	c.undecidednode++
	if c.undecidednode >= NodeID(c.localDAG.committee.Size()) {
		c.undecidednode = 0
		c.undecidedround++
	}
}

// func (c *Commitor) ifLossLiveness() bool {

// 	return true
// }

func (c *Commitor) NotifyToCommit() {
	c.notify <- struct{}{}
}
