package mempool

import (
	"WuKong/core"
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/pool"
	"WuKong/store"
	"fmt"
	"time"
)

type Mempool struct {
	Name       core.NodeID
	Committee  core.Committee
	Parameters core.Parameters
	SigService *crypto.SigService
	Store      *store.Store
	TxPool     *pool.Pool
	Transimtor *core.Transmitor
	Queue      map[crypto.Digest]struct{} //整个mempool的最大容量
	Sync       *Synchronizer
	//ConsensusMempoolCoreChan <-chan core.Messgae
}

func NewMempool(
	Name core.NodeID,
	Committee core.Committee,
	Parameters core.Parameters,
	SigService *crypto.SigService,
	Store *store.Store,
	TxPool *pool.Pool,
	Transimtor *core.Transmitor,
	Sync *Synchronizer,
	//consensusMempoolCoreChan <-chan core.Messgae,
) *Mempool {
	m := &Mempool{
		Name:       Name,
		Committee:  Committee,
		Parameters: Parameters,
		SigService: SigService,
		Store:      Store,
		TxPool:     TxPool,
		Transimtor: Transimtor,
		Queue:      make(map[crypto.Digest]struct{}),
		Sync:       Sync,
		//ConsensusMempoolCoreChan: consensusMempoolCoreChan,
	}
	return m
}

func (c *Mempool) StorePayload(payload *Payload) error {
	key := payload.Hash()
	value, err := payload.Encode()
	if err != nil {
		return err
	}
	return c.Store.Write(key[:], value)
}

func (c *Mempool) GetPayload(digest crypto.Digest) (*Payload, error) {
	value, err := c.Store.Read(digest[:])

	if err == store.ErrNotFoundKey {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	b := &Payload{}
	if err := b.Decode(value); err != nil {
		return nil, err
	}
	return b, err
}

func ErrFullMemory(author core.NodeID) error {
	return fmt.Errorf("author %d Mempool memory is full", author)
}

func (m *Mempool) payloadProcess(payload *Payload) error {

	if uint64(len(m.Queue)) >= m.Parameters.MaxMempoolQueenSize {
		return ErrFullMemory(m.Name)
	}
	//本地存储
	if err := m.StorePayload(payload); err != nil {
		return err
	}
	//转发给其他人
	message := &OtherPayloadMsg{
		Payload: payload,
	}
	m.Transimtor.Send(m.Name, core.NONE, message)
	m.Transimtor.MempololRecvChannel() <- message //????
	return nil
}

func (m *Mempool) HandleOwnPayload(payload *OwnPayloadMsg) error {
	//logger.Debug.Printf("handle mempool OwnBlockMsg\n")  自己的值先不加入自己的内存池队列，给一个缓冲的时间，尽量减少去要payload的时间
	if uint64(len(m.Queue)) >= m.Parameters.MaxMempoolQueenSize {
		return ErrFullMemory(m.Name)
	}
	digest := payload.Payload.Hash()
	if err := m.payloadProcess(payload.Payload); err != nil {
		return err
	}
	m.Queue[digest] = struct{}{}
	return nil
}

func (m *Mempool) HandleOthorPayload(payload *OtherPayloadMsg) error {
	//m.generateBlocks()
	//logger.Debug.Printf("handle mempool otherBlockMsg\n")
	if uint64(len(m.Queue)) >= m.Parameters.MaxMempoolQueenSize {
		return ErrFullMemory(m.Name)
	}

	digest := payload.Payload.Hash()
	// Verify that the payload is correctly signed.
	if flag := payload.Payload.Verify(m.Committee); !flag {
		logger.Error.Printf("Block sign error\n")
		return nil
	}
	if err := m.StorePayload(payload.Payload); err != nil {
		return err
	}
	m.Queue[digest] = struct{}{}
	return nil
}

func (m *Mempool) HandleRequestPayload(request *RequestPayloadMsg) error {
	logger.Debug.Printf("handle mempool RequestBlockMsg from %d\n", request.Author)
	for _, digest := range request.Digests {
		if p, err := m.GetPayload(digest); err != nil {
			return err
		} else {
			message := &OtherPayloadMsg{
				Payload: p,
			}
			m.Transimtor.Send(m.Name, request.Author, message) //只发给向自己要的人
		}
	}
	return nil
}

// 获取共识区块所引用的微区块
func (m *Mempool) HandleMakeBlockMsg(makemsg *MakeConsensusBlockMsg) ([]crypto.Digest, error) {
	//nums := makemsg.MaxBlockSize / uint64(len(crypto.Digest{}))
	nums := makemsg.MaxPayloadSize
	ret := make([]crypto.Digest, 0)
	if len(m.Queue) == 0 {
		logger.Debug.Printf("HandleMakeBlockMsg and len(m.Queue) == 0\n")
		payload, _ := NewPload(m.Name, m.TxPool.GetBatch(), m.SigService)
		if payload.Batch.ID != -1 {
			logger.Info.Printf("create payload node %d batch_id %d \n", payload.Proposer, payload.Batch.ID)
		}
		digest := payload.Hash()
		if err := m.payloadProcess(payload); err != nil {
			return nil, err
		}
		ret = append(ret, digest)
	} else {
		logger.Debug.Printf("HandleMakeBlockMsg and len(m.Queue) %d\n", len(m.Queue))
		for key := range m.Queue {
			ret = append(ret, key)
			nums--
			if nums == 0 {
				break
			}
		}
		//移除
		for _, key := range ret {
			delete(m.Queue, key)
		}
	}
	return ret, nil
}

func (m *Mempool) HandleCleanPayload(msg *CleanBlockMsg) error {
	//本地清理删除payload
	for _, digest := range msg.Digests {
		delete(m.Queue, digest)
	}
	//同步其他节点清理删除payload
	m.Sync.Cleanup(uint64(msg.Epoch))
	return nil
}

func (m *Mempool) HandleVerifyMsg(msg *VerifyBlockMsg) VerifyStatus {
	return m.Sync.Verify(msg.Proposer, msg.Epoch, msg.Payloads, msg.ConsensusBlockHash)
}

func (m *Mempool) generatePayload() error {
	payload, _ := NewPload(m.Name, m.TxPool.GetBatch(), m.SigService)
	if payload.Batch.ID != -1 {
		logger.Info.Printf("create payload node %d batch_id %d \n", payload.Proposer, payload.Batch.ID)
		ownmessage := &OwnPayloadMsg{
			Payload: payload,
		}
		m.Transimtor.MempololRecvChannel() <- ownmessage
	}
	// for {
	// 	block, _ := NewBlock(m.Name, m.TxPool.GetBatch(), m.SigService)
	// 	if block.Batch.ID != -1 {
	// 		logger.Info.Printf("create Block node %d batch_id %d \n", block.Proposer, block.Batch.ID)
	// 		ownmessage := &OwnBlockMsg{
	// 			Block: block,
	// 		}
	// 		m.Transimtor.MempololRecvChannel() <- ownmessage
	// 		break
	// 	}
	// }
	return nil
}

func (m *Mempool) Run() {
	//一直广播微区块,这个时间设置需要和发块速率结合起来看，也就是rust版本里面实现的一收到一个batch就立马广播并且发块
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	m.generatePayload()

	go m.Sync.Run()

	//监听mempool的消息通道

	mempoolrecvChannal := m.Transimtor.MempololRecvChannel()
	connectrecvChannal := m.Transimtor.ConnectRecvChannel()
	for {
		var err error
		select {
		case <-ticker.C:
			err = m.generatePayload()
		case msg := <-connectrecvChannal:
			{
				switch msg.MsgType() {
				case MakeBlockType:
					{
						req, _ := msg.(*MakeConsensusBlockMsg)
						data, errors := m.HandleMakeBlockMsg(req)
						req.Payloads <- data //把引用传进去了，具体使用的时候要注意,这一步要传递到哪里去？
						err = errors
					}
				case VerifyBlockType:
					{
						req, _ := msg.(*VerifyBlockMsg)
						req.Sender <- m.HandleVerifyMsg(req)
					}
				case CleanBlockType:
					{
						err = m.HandleCleanPayload(msg.(*CleanBlockMsg))
					}
				}
			}
		case msg := <-mempoolrecvChannal:
			{
				switch msg.MsgType() {
				case OwnPayloadType:
					{
						err = m.HandleOwnPayload(msg.(*OwnPayloadMsg))
					}
				case OtherPayloadType:
					{
						err = m.HandleOthorPayload(msg.(*OtherPayloadMsg))
					}
				case RequestPayloadType:
					{
						err = m.HandleRequestPayload(msg.(*RequestPayloadMsg))
					}
				}
			}
		default:
		}
		if err != nil {
			switch err.(type) {
			default:
				logger.Error.Printf("Mempool Core: %s\n", err.Error())
			}
		}
	}
}
