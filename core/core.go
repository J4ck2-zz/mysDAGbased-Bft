package core

import (
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/pool"
	"WuKong/store"
	"sync"
	"time"
)

type Core struct {
	nodeID          NodeID
	round           int
	committee       Committee
	parameters      Parameters
	txpool          *pool.Pool
	transmitor      *Transmitor
	sigService      *crypto.SigService
	store           *store.Store
	retriever       *Retriever
	commitor        *Commitor
	localDAG        *LocalDAG
	Aggreator       *Aggreator
	loopBackChannel chan *Block
	commitChannel   chan<- *Block
	proposedFlag    map[int]struct{}
	startABA        chan *ABAid
	abaInstances    map[int]map[NodeID]*ABA
	muABAIvokeFlag  *sync.RWMutex
	abaInvokeFlag   map[int]map[NodeID]map[int]map[uint8]struct{} //aba invoke flag  round Slot inround flag
	abaCallBack     chan *ABABack
	delayRoundCh    chan int
}

func NewCore(
	nodeID NodeID,
	committee Committee,
	parameters Parameters,
	txpool *pool.Pool,
	transmitor *Transmitor,
	store *store.Store,
	sigService *crypto.SigService,
	commitChannel chan<- *Block,
) *Core {
	loopBackChannel := make(chan *Block, 1_000)
	startABA := make(chan *ABAid, 1000)
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
		commitChannel:   commitChannel,
		localDAG:        NewLocalDAG(store, committee),
		Aggreator:       NewAggreator(committee, sigService),
		proposedFlag:    make(map[int]struct{}),
		startABA:        startABA,
		abaInstances:    make(map[int]map[NodeID]*ABA),
		muABAIvokeFlag:  &sync.RWMutex{},
		abaInvokeFlag:   make(map[int]map[NodeID]map[int]map[uint8]struct{}),
		abaCallBack:     make(chan *ABABack, 100),
		delayRoundCh:    make(chan int, 100),
	}

	corer.retriever = NewRetriever(nodeID, store, transmitor, sigService, parameters, loopBackChannel)
	corer.commitor = NewCommitor(corer.localDAG, store, commitChannel, startABA)

	return corer
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

func (corer *Core) checkReference(block *Block) (bool, []crypto.Digest) {
	var temp []crypto.Digest
	for d := range block.Reference {
		temp = append(temp, d)
	}
	ok, missDeigest := corer.localDAG.IsReceived(temp...)
	return ok, missDeigest
}

func (corer *Core) getABAInstance(round int, Slot NodeID) *ABA {
	items, ok := corer.abaInstances[round]
	if !ok {
		items = make(map[NodeID]*ABA)
		corer.abaInstances[round] = items
	}
	instance, ok := items[Slot]
	if !ok {
		instance = NewABA(corer, round, Slot, corer.abaCallBack)
		items[Slot] = instance
	}
	return instance
}

func (corer *Core) isInvokeABA(round int, Slotid NodeID, inRound int, tag uint8) bool {
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

func (corer *Core) hasInvokeABA(round int, Slot NodeID) bool {
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
		if round == 0 {
			block = &Block{
				Author:    corer.nodeID,
				Round:     round,
				Batch:     corer.txpool.GetBatch(),
				Reference: make(map[crypto.Digest]NodeID),
				TimeStamp: time.Now().Unix(),
			}
		} else {
			reference := corer.localDAG.GetRoundReceivedBlocks(round - 1)
			if len(reference) >= corer.committee.HightThreshold() {
				block = &Block{
					Author:    corer.nodeID,
					Round:     round,
					Batch:     corer.txpool.GetBatch(),
					Reference: reference,
					TimeStamp: time.Now().Unix(),
				}
			}
		}
	}

	if block != nil {
		corer.proposedFlag[round] = struct{}{}
		if block.Batch.Txs != nil {
			//BenchMark Log
			logger.Info.Printf("create Block round %d node %d batch_id %d \n", block.Round, block.Author, block.Batch.ID)
		}
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
	if ok, miss := corer.checkReference(propose.B); !ok {
		//retrieve miss block
		corer.retriever.requestBlocks(miss, propose.Author, propose.B.Hash())

		return ErrReference(propose.MsgType(), propose.Round, int(propose.Author))
	}

	//Step 4: write to dag

	if err := corer.handleOutPut(propose.B.Round, propose.B.Author, propose.B.Hash(), propose.B.Reference); err != nil {
		return err
	}

	return nil
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

		//maybe execute more one
		storeBlock(corer.store, block)

		corer.handleOutPut(block.Round, block.Author, block.Hash(), block.Reference)
	}

	go corer.retriever.processReply(reply)

	return nil
}

func (corer *Core) handleLoopBack(block *Block) error {
	logger.Debug.Printf("procesing block loop back round %d node %d \n", block.Round, block.Author)

	return corer.handleOutPut(block.Round, block.Author, block.Hash(), block.Reference)

}

// ABA
func (corer *Core) processABAId(m *ABAid) error {
	logger.Debug.Printf("start ABA round %d Slot %d val %d\n", m.round, m.slot, m.flag)
	return corer.invokeABAVal(m.round, m.slot, 0, m.flag, m.localstate)
}

func (corer *Core) invokeABAVal(round int, Slot NodeID, inRound int, flag uint8, localstate uint8) error {
	corer.muABAIvokeFlag.Lock()
	defer corer.muABAIvokeFlag.Unlock()
	if corer.isInvokeABA(round, Slot, inRound, flag) {
		return nil
	}
	logger.Debug.Printf("Invoke ABA round %d Slot %d in_round %d val %d\n", round, Slot, inRound, flag)
	flags, ok := corer.abaInvokeFlag[round]
	if !ok {
		flags = make(map[NodeID]map[int]map[uint8]struct{})
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
	corer.transmitor.Send(corer.nodeID, NONE, abaVal)
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
	corer.localDAG.muDAG.RLock()
	digest := corer.localDAG.localDAG[back.ExRound][back.Slot][0]
	corer.localDAG.muDAG.RUnlock()
	if back.Flag == FLAG_NO { //toskip
		corer.commitor.notifycommit <- &commitMsg{
			round:  back.ExRound,
			node:   back.Slot,
			ptern:  toskip,
			digest: crypto.Digest{},
		}
	} else if back.Flag == FLAG_YES { //tocommit
		corer.commitor.notifycommit <- &commitMsg{
			round:  back.ExRound,
			node:   back.Slot,
			ptern:  toCommit,
			digest: digest,
		}
	}
	return nil
}

func (corer *Core) handleOutPut(round int, node NodeID, digest crypto.Digest, references map[crypto.Digest]NodeID) error {
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
			corer.transmitor.Send(corer.nodeID, NONE, propose)
			corer.transmitor.RecvChannel() <- propose
		}
	}

	return nil
}

func (corer *Core) Run() {
	if corer.nodeID >= NodeID(corer.parameters.Faults) {
		//first propose
		block := corer.generatorBlock(0)
		if propose, err := NewProposeMsg(corer.nodeID, 0, block, corer.sigService); err != nil {
			logger.Error.Println(err)
			panic(err)
		} else {
			corer.transmitor.Send(corer.nodeID, NONE, propose)
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
					}

				}
			case block := <-corer.loopBackChannel:
				{
					err = corer.handleLoopBack(block)
				}
			case abaBack := <-corer.abaCallBack:
				{
					err = corer.processABABack(abaBack)
				}
			case abaid := <-corer.startABA:
				{
					err = corer.processABAId(abaid)
				}
			case round := <-corer.delayRoundCh:
				err = corer.advancedround(round)
			}

			if err != nil {
				logger.Warn.Println(err)
			}

		}
	}
}
