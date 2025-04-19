package core

import (
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/pool"
	"WuKong/store"
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
	loopBackChannel chan *Block
	commitChannel   chan<- *Block
	proposedFlag    map[int]struct{}
	// startABA        chan *ABAid
	// abaInstances    map[int]map[NodeID]*ABA
	// abaInvokeFlag   map[int64]map[int64]map[int64]map[uint8]struct{} //aba invoke flag
	// abaCallBack     chan *ABABack
	delayRoundCh chan int
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
	// startABA := make(chan *ABAid, 1000)
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
		proposedFlag:    make(map[int]struct{}),
		// startABA:        startABA,
		// abaInstances:    make(map[int]map[NodeID]*ABA),
		// abaInvokeFlag:   make(map[int64]map[int64]map[int64]map[uint8]struct{}),
		// abaCallBack:     make(chan *ABABack),
		delayRoundCh: make(chan int, 100),
	}

	corer.retriever = NewRetriever(nodeID, store, transmitor, sigService, parameters, loopBackChannel)
	corer.commitor = NewCommitor(corer.localDAG, store, commitChannel)

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

// func (corer *Core) getABAInstance(round int, nodeID NodeID) *ABA {
// 	items, ok := corer.abaInstances[round]
// 	if !ok {
// 		items = make(map[NodeID]*ABA)
// 		corer.abaInstances[round] = items
// 	}
// 	instance, ok := items[nodeID]
// 	if !ok {
// 		instance = NewABA(corer, epoch, round, corer.abaCallBack)
// 		items[nodeID] = instance
// 	}
// 	return instance
// }

// func (corer *Core) isInvokeABA(epoch, round, inRound int64, tag uint8) bool {
// 	flags, ok := corer.abaInvokeFlag[epoch]
// 	if !ok {
// 		return false
// 	}
// 	flag, ok := flags[round]
// 	if !ok {
// 		return false
// 	}
// 	item, ok := flag[inRound]
// 	if !ok {
// 		return false
// 	}
// 	_, ok = item[tag]
// 	return ok
// }

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
			}
		} else {
			reference := corer.localDAG.GetRoundReceivedBlock(round - 1)
			if len(reference) >= corer.committee.HightThreshold() {
				block = &Block{
					Author:    corer.nodeID,
					Round:     round,
					Batch:     corer.txpool.GetBatch(),
					Reference: reference,
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
	logger.Debug.Println("procesing block request")

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
// func(corer *Core) processABAId(p *ABAid){

// }

// func (corer *Core) invokeABAVal(leader NodeID, epoch, round, inRound int64, flag uint8) error {
// 	logger.Debug.Printf("Invoke ABA epoch %d ex_round %d in_round %d val %d\n", epoch, round, inRound, flag)
// 	if corer.isInvokeABA(epoch, round, inRound, flag) {
// 		return nil
// 	}
// 	flags, ok := corer.abaInvokeFlag[epoch]
// 	if !ok {
// 		flags = make(map[int64]map[int64]map[uint8]struct{})
// 		corer.abaInvokeFlag[epoch] = flags
// 	}
// 	items, ok := flags[round]
// 	if !ok {
// 		items = make(map[int64]map[uint8]struct{})
// 		flags[round] = items
// 	}
// 	item, ok := items[inRound]
// 	if !ok {
// 		item = make(map[uint8]struct{})
// 		items[inRound] = item
// 	}
// 	item[flag] = struct{}{}
// 	abaVal, _ := NewABAVal(corer.nodeID, leader, epoch, round, inRound, flag, corer.sigService)
// 	corer.transmitor.Send(corer.nodeID, NONE, abaVal)
// 	corer.transmitor.RecvChannel() <- abaVal

// 	return nil
// }

// func (corer *Core) handleABAVal(val *ABAVal) error {
// 	logger.Debug.Printf("Processing aba val leader %d epoch %d round %d in-round val %d\n", val.Leader, val.Epoch, val.Round, val.InRound, val.Flag)
// 	if corer.messageFilter(val.Epoch) {
// 		return nil
// 	}

// 	go corer.getABAInstance(val.Epoch, val.Round).ProcessABAVal(val)

// 	return nil
// }

// func (corer *Core) handleABAMux(mux *ABAMux) error {
// 	logger.Debug.Printf("Processing aba mux leader %d epoch %d round %d in-round %d val %d\n", mux.Leader, mux.Epoch, mux.Round, mux.InRound, mux.Flag)
// 	if corer.messageFilter(mux.Epoch) {
// 		return nil
// 	}

// 	go corer.getABAInstance(mux.Epoch, mux.Round).ProcessABAMux(mux)

// 	return nil
// }

// func (corer *Core) handleCoinShare(share *CoinShare) error {
// 	logger.Debug.Printf("Processing coin share epoch %d round %d in-round %d", share.Epoch, share.Round, share.InRound)
// 	if corer.messageFilter(share.Epoch) {
// 		return nil
// 	}

// 	if ok, coin, err := corer.Aggreator.addCoinShare(share); err != nil {
// 		return err
// 	} else if ok {
// 		logger.Debug.Printf("ABA epoch %d ex-round %d in-round %d coin %d\n", share.Epoch, share.Round, share.InRound, coin)
// 		go corer.getABAInstance(share.Epoch, share.Round).ProcessCoin(share.InRound, coin, share.Leader)
// 	}

// 	return nil
// }

// func (corer *Core) handleABAHalt(halt *ABAHalt) error {
// 	logger.Debug.Printf("Processing aba halt leader %d epoch %d in-round %d\n", halt.Leader, halt.Epoch, halt.InRound)
// 	if corer.messageFilter(halt.Epoch) {
// 		return nil
// 	}
// 	go corer.getABAInstance(halt.Epoch, halt.Round).ProcessHalt(halt)
// 	return nil
// }

// func (corer *Core) processABABack(back *ABABack) error {
// 	if back.Typ == ABA_INVOKE {
// 		return corer.invokeABAVal(back.Leader, back.Epoch, back.ExRound, back.InRound, back.Flag)
// 	} else if back.Typ == ABA_HALT {
// 		if back.Flag == FLAG_NO { //next round
// 			return corer.invokeStageTwo(back.Epoch, back.ExRound+1)
// 		} else if back.Flag == FLAG_YES { //next epoch
// 			return corer.handleOutPut(back.Epoch, back.Leader)
// 		}
// 	}
// 	return nil
// }

func (corer *Core) handleOutPut(round int, node NodeID, digest crypto.Digest, references map[crypto.Digest]NodeID) error {
	logger.Debug.Printf("procesing output round %d node %d \n", round, node)

	//receive block
	corer.localDAG.ReceiveBlock(round, node, digest, references)

	//try commit
	corer.commitor.NotifyToCommit()

	if n := corer.localDAG.GetRoundReceivedBlockNums(round); n == corer.committee.HightThreshold() {
		//timeout
		time.AfterFunc(time.Duration(corer.parameters.DelayProposal)*time.Millisecond, func() {
			corer.delayRoundCh <- round + 1
		})
		return nil
	} else if n == corer.committee.Size() {
		return corer.advancedround(round + 1)
	}

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
					}

				}
			case block := <-corer.loopBackChannel:
				{
					err = corer.handleLoopBack(block)
				}
			// case abaBack := <-corer.abaCallBack:
			// 	{
			// 		err = corer.processABABack(abaBack)
			// 	}
			// case abaid:=<-corer.startABA:
			// 	{
			// 		err=corer.p
			// 	}
			case round := <-corer.delayRoundCh:
				err = corer.advancedround(round)
			}

			if err != nil {
				logger.Warn.Println(err)
			}

		}
	}
}
