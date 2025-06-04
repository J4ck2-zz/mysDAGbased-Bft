package consensus

import (
	"WuKong/core"
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/mempool"
	"WuKong/pool"
	"WuKong/store"
	"sync"
	"time"
)

type Core struct {
	nodeID     core.NodeID
	round      int
	committee  core.Committee
	parameters core.Parameters
	txpool     *pool.Pool
	transmitor *core.Transmitor
	sigService *crypto.SigService
	store      *store.Store
	retriever  *Retriever
	commitor   *Commitor
	localDAG   *LocalDAG

	MemPool            *mempool.Mempool
	mempoolbackchannel chan crypto.Digest
	pendingPayloads    map[crypto.Digest]chan *mempool.Payload // digest -> waiting channel
	muPending          *sync.RWMutex

	loopBackChannel chan *Block
	loopDigests     chan crypto.Digest
	commitChannel   chan<- *Block
	proposedFlag    map[int]struct{}

	startABA       chan *ABAid
	abaInstances   map[int]map[core.NodeID]*ABA
	muABAIvokeFlag *sync.RWMutex
	abaInvokeFlag  map[int]map[core.NodeID]map[int]map[uint8]struct{} //aba invoke flag  round Slot inround flag
	abaCallBack    chan *ABABack
	Aggreator      *Aggreator

	delayRoundCh chan int
	notifyPLoad  chan crypto.Digest

	Elector      *Elector
	FinishFlags  map[int64]map[int64]map[core.NodeID]crypto.Digest // finish? map[epoch][round][node] = blockHash
	SPbInstances map[int64]map[int64]map[core.NodeID]*SPB          // map[epoch][node][round]
	DoneFlags    map[int64]map[int64]struct{}
	ReadyFlags   map[int64]map[int64]struct{}
	HaltFlags    map[int64]struct{}
	Epoch        int64
	sMVBAStart   chan *SMVBAid
	sMVBAValue   map[crypto.Digest]*SmvbaValue
}

func NewCore(
	nodeID core.NodeID,
	committee core.Committee,
	parameters core.Parameters,
	txpool *pool.Pool,
	transmitor *core.Transmitor,
	store *store.Store,
	sigService *crypto.SigService,
	commitChannel chan<- *Block,
) *Core {
	loopBackChannel := make(chan *Block, 1_000)
	startABA := make(chan *ABAid, 1000)
	loopDigest := make(chan crypto.Digest, 100)
	mempoolbackchannel := make(chan crypto.Digest, 100)
	notifypload := make(chan crypto.Digest, 100)
	sMVBAStart := make(chan *SMVBAid)

	Sync := mempool.NewSynchronizer(nodeID, transmitor, mempoolbackchannel, store)
	pool := mempool.NewMempool(nodeID, committee, parameters, sigService, store, txpool, transmitor, Sync)
	corer := &Core{
		nodeID:          nodeID,
		committee:       committee,
		round:           0,
		parameters:      parameters,
		txpool:          txpool,
		transmitor:      transmitor,
		sigService:      sigService,
		store:           store,
		loopBackChannel: loopBackChannel,
		loopDigests:     loopDigest,
		commitChannel:   commitChannel,

		MemPool:            pool,
		pendingPayloads:    make(map[crypto.Digest]chan *mempool.Payload),
		muPending:          &sync.RWMutex{},
		mempoolbackchannel: mempoolbackchannel,

		localDAG:       NewLocalDAG(store, committee),
		Aggreator:      NewAggreator(committee, sigService),
		proposedFlag:   make(map[int]struct{}),
		startABA:       startABA,
		abaInstances:   make(map[int]map[core.NodeID]*ABA),
		muABAIvokeFlag: &sync.RWMutex{},
		abaInvokeFlag:  make(map[int]map[core.NodeID]map[int]map[uint8]struct{}),
		abaCallBack:    make(chan *ABABack, 100),

		delayRoundCh: make(chan int, 100),
		notifyPLoad:  notifypload,

		Elector:      NewElector(sigService, committee),
		FinishFlags:  make(map[int64]map[int64]map[core.NodeID]crypto.Digest),
		SPbInstances: make(map[int64]map[int64]map[core.NodeID]*SPB),
		DoneFlags:    make(map[int64]map[int64]struct{}),
		ReadyFlags:   make(map[int64]map[int64]struct{}),
		HaltFlags:    make(map[int64]struct{}),
		sMVBAStart:   sMVBAStart,
		sMVBAValue:   make(map[crypto.Digest]*SmvbaValue),
	}

	corer.retriever = NewRetriever(nodeID, store, transmitor, sigService, parameters, loopBackChannel, loopDigest, corer.localDAG)
	corer.commitor = NewCommitor(corer.localDAG, store, commitChannel, startABA, transmitor, notifypload, sMVBAStart)

	return corer
}

func GetPayload(s *store.Store, digest crypto.Digest) (*mempool.Payload, error) {
	value, err := s.Read(digest[:])

	if err == store.ErrNotFoundKey {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	b := &mempool.Payload{}
	if err := b.Decode(value); err != nil {
		return nil, err
	}
	return b, err
}

func storeBlock(store *store.Store, block *Block) error {
	key := block.Hash()
	if val, err := block.Encode(); err != nil {
		return err
	} else {
		store.Write(key[:], val)
		return nil
	}
}

func getBlock(store *store.Store, digest crypto.Digest) (*Block, error) {
	block := &Block{}
	data, err := store.Read(digest[:])
	if err != nil {
		return nil, err
	}
	if err := block.Decode(data); err != nil {
		return nil, err
	}
	return block, nil
}

func isInStore(store *store.Store, digest crypto.Digest) bool {
	_, err := store.Read(digest[:])

	return err == nil
}

func (corer *Core) checkReference(block *Block) (bool, []crypto.Digest, []crypto.Digest) {
	var temp []crypto.Digest
	var localMissBlocks []crypto.Digest
	var remoteMissBlocks []crypto.Digest
	for d := range block.Reference {
		temp = append(temp, d)
	}
	ok, missDeigest := corer.localDAG.IsReceived(temp...)

	if !ok {
		for _, d := range missDeigest {
			if isInStore(corer.store, d) {
				localMissBlocks = append(localMissBlocks, d)
			} else {
				remoteMissBlocks = append(remoteMissBlocks, d)
			}
		}
		return ok, remoteMissBlocks, localMissBlocks
	}

	return ok, missDeigest, localMissBlocks
}

func (corer *Core) getABAInstance(round int, Slot core.NodeID) *ABA {
	items, ok := corer.abaInstances[round]
	if !ok {
		items = make(map[core.NodeID]*ABA)
		corer.abaInstances[round] = items
	}
	instance, ok := items[Slot]
	if !ok {
		instance = NewABA(corer, round, Slot, corer.abaCallBack)
		items[Slot] = instance
	}
	return instance
}

func (corer *Core) isInvokeABA(round int, Slotid core.NodeID, inRound int, tag uint8) bool {
	flags, ok := corer.abaInvokeFlag[round]
	if !ok {
		return false
	}
	flag, ok := flags[Slotid]
	if !ok {
		return false
	}
	item, ok := flag[inRound]
	if !ok {
		return false
	}
	_, ok = item[tag]
	return ok
}

func (corer *Core) hasInvokeABA(round int, Slot core.NodeID) bool {
	corer.muABAIvokeFlag.RLock()
	defer corer.muABAIvokeFlag.RUnlock()
	flags, ok := corer.abaInvokeFlag[round]
	if !ok {
		return false
	}
	_, ok = flags[Slot]

	return ok
}

// func (corer *Core) messageFilter(epoch int64) bool {
// 	return corer.Epoch > epoch
// }

/*********************************Protocol***********************************************/
func (corer *Core) generatorBlock(round int) *Block {
	logger.Debug.Printf("procesing generatorBlock round %d \n", round)

	var block *Block
	if _, ok := corer.proposedFlag[round]; !ok {
		referencechan := make(chan []crypto.Digest)
		msg := &mempool.MakeConsensusBlockMsg{
			MaxPayloadSize: uint64(MAXCOUNT), Payloads: referencechan,
		}
		corer.transmitor.ConnectRecvChannel() <- msg
		payloads := <-referencechan
		if round == 0 {
			block = &Block{
				Author:    corer.nodeID,
				Round:     round,
				PayLoads:  payloads,
				Reference: make(map[crypto.Digest]core.NodeID),
				TimeStamp: time.Now().Unix(),
			}
		} else {
			reference := corer.localDAG.GetRoundReceivedBlocks(round - 1)
			if len(reference) >= corer.committee.HightThreshold() {
				block = &Block{
					Author:    corer.nodeID,
					Round:     round,
					PayLoads:  payloads,
					Reference: reference,
					//Reference: make(map[crypto.Digest]core.NodeID),
					TimeStamp: time.Now().Unix(),
				}
			}
		}
	}

	//BenchMark Log

	if block != nil {
		corer.proposedFlag[round] = struct{}{}
		logger.Info.Printf("create Block round %d node %d \n", block.Round, block.Author)
	}

	return block

}

func (corer *Core) handlePropose(propose *ProposeMsg) error {
	logger.Debug.Printf("procesing block propose round %d node %d \n", propose.Round, propose.Author)

	//Step 1: verify signature
	if !propose.Verify(corer.committee) {
		return ErrSignature(propose.MsgType(), propose.Round, int(propose.Author))
	}

	//Step 2: store Block
	if err := storeBlock(corer.store, propose.B); err != nil {
		return err
	}

	//Step 3: check reference
	if ok, miss, localMiss := corer.checkReference(propose.B); !ok {
		//retrieve miss block
		corer.retriever.requestBlocks(miss, localMiss, propose.Author, propose.B.Hash())

		if status := corer.checkPayloads(propose.B); status != mempool.OK {
			logger.Debug.Printf("[round-%d-node-%d] not receive all payloads\n ", propose.Round, propose.Author)
		}
		if localMiss != nil {
			return ErrLocalReference(propose.MsgType(), propose.Round, int(propose.Author), len(miss), len(localMiss))
		}
		return ErrReference(propose.MsgType(), propose.Round, int(propose.Author))
	}

	//Step 4:check payloads
	if status := corer.checkPayloads(propose.B); status != mempool.OK {
		return ErrLossPayloads(propose.Round, int(propose.Author))
	}

	//Step 5: write to dag
	if err := corer.handleOutPut(propose.B.Round, propose.B.Author, propose.B.Hash(), propose.B.Reference); err != nil {
		return err
	}

	return nil
}

func (corer *Core) checkPayloads(block *Block) mempool.VerifyStatus {
	msg := &mempool.VerifyBlockMsg{
		Proposer:           block.Author,
		Epoch:              int64(block.Round),
		Payloads:           block.PayLoads,
		ConsensusBlockHash: block.Hash(),
		Sender:             make(chan mempool.VerifyStatus),
	}
	corer.transmitor.ConnectRecvChannel() <- msg
	status := <-msg.Sender
	return status
}

func (corer *Core) handleRequestBlock(request *RequestBlockMsg) error {
	logger.Debug.Printf("procesing block request from node %d", request.Author)

	//Step 1: verify signature
	if !request.Verify(corer.committee) {
		return ErrSignature(request.MsgType(), -1, int(request.Author))
	}

	go corer.retriever.processRequest(request)

	return nil
}

func (corer *Core) handleReplyBlock(reply *ReplyBlockMsg) error {
	logger.Debug.Println("procesing block reply")

	//Step 1: verify signature
	if !reply.Verify(corer.committee) {
		return ErrSignature(reply.MsgType(), -1, int(reply.Author))
	}

	for _, block := range reply.Blocks {

		status := corer.checkPayloads(block)
		if status != mempool.OK {
			continue
		}
		//maybe execute more one
		storeBlock(corer.store, block)

		corer.handleOutPut(block.Round, block.Author, block.Hash(), block.Reference)
	}

	go corer.retriever.processReply(reply)

	return nil
}

func (corer *Core) handleLoopBack(block *Block) error {
	logger.Debug.Printf("procesing block loop back round %d node %d \n", block.Round, block.Author)
	status := corer.checkPayloads(block)
	if status != mempool.OK {
		return ErrLossPayloads(block.Round, int(block.Author))
	}
	err := corer.handleOutPut(block.Round, block.Author, block.Hash(), block.Reference)
	if err == nil {
		corer.loopDigests <- block.Hash()
		logger.Warn.Printf("loopback round-%d-node-%d  \n", block.Round, block.Author)
	}
	return nil
}

// mempool
func (corer *Core) handleMLoopBack(digest crypto.Digest) error {
	corer.notifyPLoad <- digest

	//re output
	block, _ := getBlock(corer.store, digest)

	if ok, _, _ := corer.checkReference(block); ok {
		err := corer.handleOutPut(block.Round, block.Author, block.Hash(), block.Reference)
		if err == nil {
			corer.loopDigests <- block.Hash()
			logger.Warn.Printf("mloopback round-%d-node-%d  \n", block.Round, block.Author)
		}
		return err
	}

	return nil
}

// ABA
func (corer *Core) processABAId(m *ABAid) error {
	logger.Debug.Printf("start ABA round %d Slot %d val %d\n", m.round, m.slot, m.flag)
	return corer.invokeABAVal(m.round, m.slot, 0, m.flag, m.localstate)
}

func (corer *Core) invokeABAVal(round int, Slot core.NodeID, inRound int, flag uint8, localstate uint8) error {
	corer.muABAIvokeFlag.Lock()
	defer corer.muABAIvokeFlag.Unlock()
	if corer.isInvokeABA(round, Slot, inRound, flag) {
		return nil
	}
	logger.Debug.Printf("Invoke ABA round %d Slot %d in_round %d val %d\n", round, Slot, inRound, flag)
	flags, ok := corer.abaInvokeFlag[round]
	if !ok {
		flags = make(map[core.NodeID]map[int]map[uint8]struct{})
		corer.abaInvokeFlag[round] = flags
	}
	items, ok := flags[Slot]
	if !ok {
		items = make(map[int]map[uint8]struct{})
		flags[Slot] = items
	}
	item, ok := items[inRound]
	if !ok {
		item = make(map[uint8]struct{})
		items[inRound] = item
	}
	item[flag] = struct{}{}
	abaVal, _ := NewABAVal(corer.nodeID, round, Slot, inRound, flag, corer.sigService, localstate)
	corer.transmitor.Send(corer.nodeID, core.NONE, abaVal)
	corer.transmitor.RecvChannel() <- abaVal

	return nil
}

func (corer *Core) handleABAVal(val *ABAVal) error {
	logger.Debug.Printf("Processing aba val round %d Slot %d in-round %d val %d from %d\n", val.Round, val.Slot, val.InRound, val.Flag, val.Author)

	go corer.getABAInstance(val.Round, val.Slot).ProcessABAVal(val)

	if val.InRound == 0 {
		if !corer.hasInvokeABA(val.Round, val.Slot) {

			go corer.passivelyStartABA(val)
		}
	}

	return nil
}

func (corer *Core) passivelyStartABA(val *ABAVal) {
	for {
		ptern := corer.commitor.IsReceivePattern(val.Round, val.Slot)
		logger.Debug.Printf("Check pattern at round %d slot %d: %v", val.Round, val.Slot, ptern)
		if ptern == toskip {
			logger.Debug.Printf("passively create aba val round %d Slot %d in-round %d  flag 1", val.Round, val.Slot, val.InRound)
			corer.invokeABAVal(val.Round, val.Slot, val.InRound, FLAG_NO, FLAG_YES)
			break
		} else if ptern == toCommit {
			logger.Debug.Printf("passively create aba val round %d Slot %d in-round %d  flag 0", val.Round, val.Slot, val.InRound)
			corer.invokeABAVal(val.Round, val.Slot, val.InRound, FLAG_YES, FLAG_YES)
			break
		} else if ptern == undecide {
			break
		} else if ptern == unjudge {

			time.Sleep(time.Millisecond * time.Duration(corer.parameters.JudgeDelay))
		}
	}
}

func (corer *Core) handleABAMux(mux *ABAMux) error {
	logger.Debug.Printf("Processing aba mux round %d solt %d in-round %d val %d\n", mux.Round, mux.Slot, mux.InRound, mux.Flag)

	go corer.getABAInstance(mux.Round, mux.Slot).ProcessABAMux(mux)

	return nil
}

func (corer *Core) handleCoinShare(share *CoinShare) error {
	logger.Debug.Printf("Processing coin share round %d Slot %d in-round %d", share.Round, share.Slot, share.InRound)

	if ok, coin, err := corer.Aggreator.addCoinShare(share); err != nil {
		return err
	} else if ok {
		logger.Debug.Printf("ABA  round %d Slot %d in-round %d coin %d\n", share.Round, share.Slot, share.InRound, coin)
		go corer.getABAInstance(share.Round, share.Slot).ProcessCoin(share.InRound, coin)
	}

	return nil
}

func (corer *Core) handleABAHalt(halt *ABAHalt) error {
	logger.Debug.Printf("Processing aba halt round %d Slot %d in-round %d flag %d\n", halt.Round, halt.Slot, halt.InRound, halt.Flag)
	go corer.getABAInstance(halt.Round, halt.Slot).ProcessHalt(halt)
	return nil
}

func (corer *Core) processABABack(back *ABABack) error {
	if back.Typ == ABA_INVOKE {
		return corer.invokeABAVal(back.ExRound, back.Slot, back.InRound, back.Flag, FLAG_NO)
	} else if back.Typ == ABA_HALT {
		return corer.receivePatternCallback(back)
	}
	return nil
}

func (corer *Core) receivePatternCallback(back *ABABack) error {
	logger.Debug.Printf("Processing ababack round %d Slot %d in-round %d flag %d\n", back.ExRound, back.Slot, back.InRound, back.Flag)

	if back.Flag == FLAG_NO { //toskip
		corer.commitor.notifycommit <- &commitMsg{
			round:  back.ExRound,
			node:   back.Slot,
			ptern:  toskip,
			digest: crypto.Digest{},
		}
	} else if back.Flag == FLAG_YES { //tocommit
		corer.localDAG.muDAG.RLock()
		digest := corer.localDAG.localDAG[back.ExRound][back.Slot][0]
		corer.localDAG.muDAG.RUnlock()
		corer.commitor.notifycommit <- &commitMsg{
			round:  back.ExRound,
			node:   back.Slot,
			ptern:  toCommit,
			digest: digest,
		}
	}
	return nil
}

func (corer *Core) handleOutPut(round int, node core.NodeID, digest crypto.Digest, references map[crypto.Digest]core.NodeID) error {
	logger.Debug.Printf("procesing output round %d node %d \n", round, node)

	//receive block
	corer.localDAG.ReceiveBlock(round, node, digest, references)
	// try judge
	corer.commitor.NotifyToJudge()
	if n := corer.localDAG.GetRoundReceivedBlockNums(round); n >= corer.committee.HightThreshold() {

		return corer.advancedround(round + 1)
	}

	// if n := corer.localDAG.GetRoundReceivedBlockNums(round); n == corer.committee.HightThreshold() {
	// 	//timeout
	// 	time.AfterFunc(time.Duration(corer.parameters.DelayProposal)*time.Millisecond, func() {
	// 		corer.delayRoundCh <- round + 1
	// 	})
	// 	return nil
	// } else if n == corer.committee.Size() {
	// 	return corer.advancedround(round + 1)
	// }

	return nil
}

func (corer *Core) advancedround(round int) error {
	logger.Debug.Printf("procesing advance round %d \n", round)

	if block := corer.generatorBlock(round); block != nil {
		if propose, err := NewProposeMsg(corer.nodeID, round, block, corer.sigService); err != nil {
			return err
		} else {
			// time.AfterFunc(time.Duration(corer.parameters.NetwrokDelay)*time.Millisecond, func() {
			// 	corer.transmitor.Send(corer.nodeID, core.NONE, propose)
			// 	corer.transmitor.RecvChannel() <- propose
			// })
			corer.transmitor.Send(corer.nodeID, core.NONE, propose)
			corer.transmitor.RecvChannel() <- propose
		}
	}

	return nil
}

func (corer *Core) sMVBARun() {
	for {
		msg := <-corer.transmitor.SMVBARecvChannel()
		var err error
		switch msg.MsgType() {
		case SPBProposalType:
			start := time.Now()
			err = corer.handleSpbProposal(msg.(*SPBProposal))
			logger.Error.Printf("handleSpbProposal took %v", time.Since(start))
		case SPBVoteType:
			start := time.Now()
			err = corer.handleSpbVote(msg.(*SPBVote))
			logger.Error.Printf("handleSpbvote took %v", time.Since(start))
		case FinishType:
			start := time.Now()
			err = corer.handleFinish(msg.(*Finish))
			logger.Error.Printf("handleFinish took %v", time.Since(start))
		case DoneType:
			start := time.Now()
			err = corer.handleDone(msg.(*Done))
			logger.Error.Printf("handleDone took %v", time.Since(start))
		case ElectShareType:
			start := time.Now()
			err = corer.handleElectShare(msg.(*ElectShare))
			logger.Error.Printf("handleElectshare took %v", time.Since(start))
		case PrevoteType:
			start := time.Now()
			err = corer.handlePrevote(msg.(*Prevote))
			logger.Error.Printf("handlePrevote took %v", time.Since(start))
		case FinVoteType:
			start := time.Now()
			err = corer.handleFinvote(msg.(*FinVote))
			logger.Error.Printf("handleFinvote took %v", time.Since(start))
		case HaltType:
			start := time.Now()
			err = corer.handleHalt(msg.(*Halt))
			logger.Error.Printf("handleHalt took %v", time.Since(start))
		}
		if err != nil {
			logger.Warn.Println(err)
		}
	}

}

func (corer *Core) Run() {
	if corer.nodeID >= core.NodeID(corer.parameters.Faults) {
		//启动mempool
		go corer.MemPool.Run()
		//first propose
		go corer.sMVBARun()
		block := corer.generatorBlock(0)
		if propose, err := NewProposeMsg(corer.nodeID, 0, block, corer.sigService); err != nil {
			logger.Error.Println(err)
			panic(err)
		} else {
			corer.transmitor.Send(corer.nodeID, core.NONE, propose)
			corer.transmitor.RecvChannel() <- propose
		}

		for {
			var err error
			select {
			case msg := <-corer.transmitor.RecvChannel():
				{
					switch msg.MsgType() {

					case ProposeMsgType:
						err = corer.handlePropose(msg.(*ProposeMsg))
					case RequestBlockType:
						err = corer.handleRequestBlock(msg.(*RequestBlockMsg))
					case ReplyBlockType:
						err = corer.handleReplyBlock(msg.(*ReplyBlockMsg))
					case ABAValType:
						err = corer.handleABAVal(msg.(*ABAVal))
					case ABAMuxType:
						err = corer.handleABAMux(msg.(*ABAMux))
					case ABAHaltType:
						err = corer.handleABAHalt(msg.(*ABAHalt))
					case CoinShareType:
						err = corer.handleCoinShare(msg.(*CoinShare))

						// case SPBProposalType:
						// 	start := time.Now()
						// 	err = corer.handleSpbProposal(msg.(*SPBProposal))
						// 	logger.Error.Printf("handleSpbProposal took %v", time.Since(start))
						// case SPBVoteType:
						// 	start := time.Now()
						// 	err = corer.handleSpbVote(msg.(*SPBVote))
						// 	logger.Error.Printf("handleSpbvote took %v", time.Since(start))
						// case FinishType:
						// 	start := time.Now()
						// 	err = corer.handleFinish(msg.(*Finish))
						// 	logger.Error.Printf("handleFinish took %v", time.Since(start))
						// case DoneType:
						// 	start := time.Now()
						// 	err = corer.handleDone(msg.(*Done))
						// 	logger.Error.Printf("handleDone took %v", time.Since(start))
						// case ElectShareType:
						// 	start := time.Now()
						// 	err = corer.handleElectShare(msg.(*ElectShare))
						// 	logger.Error.Printf("handleElectshare took %v", time.Since(start))
						// case PrevoteType:
						// 	start := time.Now()
						// 	err = corer.handlePrevote(msg.(*Prevote))
						// 	logger.Error.Printf("handlePrevote took %v", time.Since(start))
						// case FinVoteType:
						// 	start := time.Now()
						// 	err = corer.handleFinvote(msg.(*FinVote))
						// 	logger.Error.Printf("handleFinvote took %v", time.Since(start))
						// case HaltType:
						// 	start := time.Now()
						// 	err = corer.handleHalt(msg.(*Halt))
						// 	logger.Error.Printf("handleHalt took %v", time.Since(start))
					}

				}
			case round := <-corer.delayRoundCh:
				{
					err = corer.advancedround(round)
				}
			case block := <-corer.loopBackChannel:
				{
					err = corer.handleLoopBack(block)
				}
			case mblock := <-corer.mempoolbackchannel:
				{
					logger.Debug.Printf("mempoolbackchannel receive \n")
					err = corer.handleMLoopBack(mblock)
				}
			case abaBack := <-corer.abaCallBack:
				{
					err = corer.processABABack(abaBack)
				}
			case abaid := <-corer.startABA:
				{
					err = corer.processABAId(abaid)
				}
			case sMVBAid := <-corer.sMVBAStart:
				{
					err = corer.handlesMVBAStart(sMVBAid)
				}

			}

			if err != nil {
				logger.Warn.Println(err)
			}

		}
	}
}
