package core

import (
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/store"
	"sync"
)

type pattern int

const (
	unjudge pattern = iota
	toskip
	undecide
	toCommit
)

type LocalDAG struct {
	store        *store.Store
	committee    Committee
	muBlock      *sync.RWMutex
	blockDigests map[crypto.Digest]NodeID // store hash of block that has received
	muDAG        *sync.RWMutex
	localDAG     map[int]map[NodeID][]crypto.Digest // local DAG
	edgesDAG     map[int]map[NodeID][]map[crypto.Digest]NodeID
	muCert       *sync.RWMutex
	iscertDAG    map[int]map[NodeID]pattern //proposer pattern
}

func NewLocalDAG(store *store.Store, committee Committee) *LocalDAG {
	return &LocalDAG{
		muBlock:      &sync.RWMutex{},
		muDAG:        &sync.RWMutex{},
		muCert:       &sync.RWMutex{},
		blockDigests: make(map[crypto.Digest]NodeID),
		localDAG:     make(map[int]map[NodeID][]crypto.Digest),
		edgesDAG:     make(map[int]map[NodeID][]map[crypto.Digest]NodeID),
		iscertDAG:    make(map[int]map[NodeID]pattern),
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
	local.muBlock.RLock()
	if _, ok := local.blockDigests[digest]; ok {
		local.muBlock.RUnlock()
		return
	}
	local.muBlock.RUnlock()

	logger.Debug.Printf("ReceiveBlock: round=%d node=%d digest=%x", round, node, digest)
	local.muBlock.Lock()
	local.blockDigests[digest] = node
	local.muBlock.Unlock()

	local.muDAG.Lock()

	vslot, ok := local.localDAG[round]
	if !ok {
		vslot = make(map[NodeID][]crypto.Digest)
		local.localDAG[round] = vslot
	}

	eslot, ok := local.edgesDAG[round]
	if !ok {
		eslot = make(map[NodeID][]map[crypto.Digest]NodeID)
		local.edgesDAG[round] = eslot
	}

	certslot, ok := local.iscertDAG[round]
	if !ok {
		certslot = make(map[NodeID]pattern)
		local.iscertDAG[round] = certslot
	}

	vslot[node] = append(vslot[node], digest)
	eslot[node] = append(eslot[node], references)
	certslot[node] = unjudge
	local.muDAG.Unlock()
}

func (local *LocalDAG) GetRoundReceivedBlockNums(round int) (nums int) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()

	nums = len(local.localDAG[round])

	return
}

func (local *LocalDAG) GetReceivedBlock(round int, node NodeID) ([]crypto.Digest, bool) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()
	if slot, ok := local.localDAG[round]; ok {
		d, ok := slot[node]
		return d, ok
	}
	return nil, false
}

func (local *LocalDAG) GetReceivedBlockReference(round int, node NodeID) (map[crypto.Digest]NodeID, bool) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()
	if slot, ok := local.edgesDAG[round]; ok {
		reference, ok := slot[node]
		if ok {
			return reference[0], ok
		}
	}
	return nil, false
}

func (local *LocalDAG) GetRoundReceivedBlock(round int) map[NodeID][]crypto.Digest {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()

	original := local.localDAG[round]

	copied := make(map[NodeID][]crypto.Digest, len(original))
	for k, v := range original {
		tmp := make([]crypto.Digest, len(v))
		copy(tmp, v)
		copied[k] = tmp
	}

	return copied
}

func (local *LocalDAG) GetRoundReceivedBlocks(round int) (references map[crypto.Digest]NodeID) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()

	blocks := local.localDAG[round]

	references = make(map[crypto.Digest]NodeID)
	for id, digests := range blocks {
		references[digests[0]] = id
	}
	return references
}

// judge if bvote refer to bproposer
func (local *LocalDAG) isVote(Bproposer *Block, Bvote *Block) bool {
	id := Bproposer.Author
	r := Bproposer.Round

	return local.supportedblock(Bvote, id, r) == Bproposer.Hash()

}

func (local *LocalDAG) supportedblock(b *Block, id NodeID, r int) crypto.Digest {

	digests := b.Reference

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
	digests := Bcert.Reference
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

func (local *LocalDAG) GetVotingBlocks(round int) map[NodeID][]crypto.Digest {

	return local.GetRoundReceivedBlock(round)

}

func (local *LocalDAG) GetDecisonBlocks(b *Block) map[NodeID][]crypto.Digest {

	return local.GetRoundReceivedBlock(b.Round + 2)

}

func (local *LocalDAG) skippedProposer(id NodeID, round int) bool {
	var res int = 0

	blocks := local.GetVotingBlocks(round + 1)
	for _, digests := range blocks {
		var flag bool = true
		for _, digest := range digests {
			if block, err := getBlock(local.store, digest); err != nil {
				logger.Warn.Println(err)
			} else {
				if containsValue(block.Reference, id) {
					flag = false
					break
				}
			}
		}
		if flag {
			res++
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

func (local *LocalDAG) committedProposer(b *Block) (bool, int, crypto.Digest) {
	var certCount int = 0
	var lastCert crypto.Digest
	blocks := local.GetDecisonBlocks(b)
	for _, digests := range blocks {
		for _, digest := range digests {
			if block, err := getBlock(local.store, digest); err != nil {
				logger.Warn.Println(err)
				continue
			} else {
				if local.isCert(b, block) {
					lastCert = digest
					certCount++
					break
				}
			}
		}
	}

	return certCount >= local.committee.HightThreshold(), certCount, lastCert
}

type ABAid struct {
	round      int
	slot       NodeID
	flag       uint8
	localstate uint8
	abablock   crypto.Digest
	certblock  crypto.Digest
}

type commitMsg struct {
	round  int
	node   NodeID
	ptern  pattern
	digest crypto.Digest
}

type Commitor struct {
	commitChannel chan<- *Block
	localDAG      *LocalDAG
	commitBlocks  map[crypto.Digest]struct{}
	notify        chan struct{}
	notifycommit  chan *commitMsg
	inner         chan crypto.Digest
	store         *store.Store
	startABA      chan<- *ABAid
	commitRound   int
	commitNode    NodeID
	judinground   int
	judingnode    NodeID
	mucDAG        *sync.RWMutex
	commitDAG     map[int]map[NodeID]crypto.Digest
	certDAG       map[int]map[NodeID]pattern
}

func NewCommitor(localDAG *LocalDAG, store *store.Store, commitChannel chan<- *Block, startABA chan<- *ABAid) *Commitor {
	c := &Commitor{
		mucDAG:        &sync.RWMutex{},
		localDAG:      localDAG,
		commitChannel: commitChannel,
		commitBlocks:  make(map[crypto.Digest]struct{}),
		notify:        make(chan struct{}, 100),
		commitDAG:     make(map[int]map[NodeID]crypto.Digest),
		certDAG:       make(map[int]map[NodeID]pattern),
		notifycommit:  make(chan *commitMsg, 1000),
		store:         store,
		inner:         make(chan crypto.Digest, 100),
		startABA:      startABA,
		commitRound:   0,
		commitNode:    0,
		judinground:   0,
		judingnode:    0,
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

	go func() {
		for m := range c.notifycommit {
			c.receivePattern(m)
		}
	}()

	for range c.notify {
		c.judgePattern()

	}
}

func (c *Commitor) judgePattern() {
	for {

		if c.localDAG.GetRoundReceivedBlockNums(c.judinground+1) >= c.localDAG.committee.HightThreshold() {
			logger.Debug.Printf("judgding  round %d node %d  \n", c.judinground, c.judingnode)
			if c.localDAG.skippedProposer(c.judingnode, c.judinground) {
				logger.Debug.Printf("judge  round %d node %d toskip \n", c.judinground, c.judingnode)
				c.notifycommit <- &commitMsg{
					round:  c.judinground,
					node:   c.judingnode,
					ptern:  toskip,
					digest: crypto.Digest{},
				}
				c.advancedJudingPointer()
				// if c.ifLossLiveness() {

				// }
				continue
			} else if c.localDAG.GetRoundReceivedBlockNums(c.judinground+2) >= c.localDAG.committee.HightThreshold() {
				c.localDAG.muDAG.RLock()
				digests := c.localDAG.localDAG[c.judinground][c.judingnode]
				c.localDAG.muDAG.RUnlock()

				var certsNum []int
				var certDigest []crypto.Digest
				var ifCommit bool = false
				for _, digest := range digests {
					if block, err := getBlock(c.store, digest); err != nil {
						logger.Warn.Println(err)
					} else {
						flag, nums, cert := c.localDAG.committedProposer(block)
						if flag {
							c.notifycommit <- &commitMsg{
								round:  c.judinground,
								node:   c.judingnode,
								ptern:  toCommit,
								digest: digest,
							}

							logger.Debug.Printf("judge  round %d node %d  tocommit \n", c.judinground, c.judingnode)
							ifCommit = true
							c.advancedJudingPointer()
							break
						}
						certsNum = append(certsNum, nums)
						certDigest = append(certDigest, cert)
					}
				}

				if !ifCommit {
					logger.Debug.Printf("undecided round %d node %d \n", c.judinground, c.judingnode)
					c.notifycommit <- &commitMsg{
						round:  c.judinground,
						node:   c.judingnode,
						ptern:  undecide,
						digest: crypto.Digest{},
					}
					var isSend bool = false
					for ix, n := range certsNum {
						if n > 0 {
							c.startABA <- &ABAid{
								round:      c.judinground,
								slot:       c.judingnode,
								flag:       FLAG_YES,
								localstate: FLAG_YES,
								abablock:   digests[ix],
								certblock:  certDigest[ix],
							}
							isSend = true
							break
						}
					}
					if !isSend {
						c.startABA <- &ABAid{
							round:      c.judinground,
							slot:       c.judingnode,
							flag:       FLAG_NO,
							localstate: FLAG_YES,
							abablock:   crypto.Digest{},
							certblock:  crypto.Digest{},
						}
					}
					c.advancedJudingPointer()
				}
			} else { //no 2f+1 decisionblocks
				break
			}
		} else { //no 2f+1 votingblocks
			break
		}

	}

}

func (c *Commitor) tryToCommit() {
	for {
		ptern, ok := c.certDAG[c.commitRound]

		if !ok {
			break
		} else {
			if ptern[c.commitNode] == toCommit {
				logger.Debug.Printf("commit round %d node %d \n", c.commitRound, c.commitNode)
				digest := c.commitDAG[c.commitRound][c.commitNode]
				c.inner <- digest
				c.advancedCommitPointer()
			} else if ptern[c.commitNode] == toskip {
				logger.Debug.Printf("skip round %d node %d \n", c.commitRound, c.commitNode)
				c.advancedCommitPointer()
			} else if ptern[c.commitNode] == undecide {
				break
			} else if ptern[c.commitNode] == unjudge {
				break
			}
		}

	}

}

func (c *Commitor) receivePattern(m *commitMsg) {
	c.mucDAG.Lock()
	defer c.mucDAG.Unlock()
	if _, ok := c.certDAG[m.round]; !ok {
		c.certDAG[m.round] = make(map[NodeID]pattern)
	}
	c.certDAG[m.round][m.node] = m.ptern

	if m.ptern == toCommit {
		if _, ok := c.commitDAG[m.round]; !ok {
			c.commitDAG[m.round] = make(map[NodeID]crypto.Digest)
		}
		c.commitDAG[m.round][m.node] = m.digest
	}
	c.tryToCommit()
}

func (c *Commitor) IsReceivePattern(round int, slot NodeID) pattern {
	c.mucDAG.RLock()
	defer c.mucDAG.RUnlock()
	item, ok := c.certDAG[round]
	if !ok {
		return unjudge
	}
	ptern, ok := item[slot]
	if !ok {
		return unjudge
	}
	return ptern
}

func (c *Commitor) advancedJudingPointer() {
	c.judingnode++
	if c.judingnode >= NodeID(c.localDAG.committee.Size()) {
		c.judingnode = 0
		c.judinground++
	}
}

func (c *Commitor) advancedCommitPointer() {
	c.commitNode++
	if c.commitNode >= NodeID(c.localDAG.committee.Size()) {
		c.commitNode = 0
		c.commitRound++
	}
}

func (c *Commitor) NotifyToJudge() {
	c.notify <- struct{}{}
}

// func (c *Commitor) ifLossLiveness() bool {

//		return true
//	}
