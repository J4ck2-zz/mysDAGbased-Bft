package core

import (
	"WuKong/crypto"
	"WuKong/pool"
	"bytes"
	"encoding/gob"
	"reflect"
	"strconv"
)

const (
	FLAG_YES uint8 = 0
	FLAG_NO  uint8 = 1
)

type ConsensusMessage interface {
	MsgType() int
	Hash() crypto.Digest
}

type Block struct {
	Author    NodeID
	Round     int
	Batch     pool.Batch
	Reference map[crypto.Digest]NodeID
}

func (b *Block) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *Block) Decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(b); err != nil {
		return err
	}
	return nil
}

func (b *Block) Hash() crypto.Digest {

	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(b.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(b.Round), 2))
	for _, tx := range b.Batch.Txs {
		hasher.Add(tx)
	}
	// for d, id := range b.Reference {
	// 	hasher.Add(d[:])
	// 	hasher.Add(strconv.AppendInt(nil, int64(id), 2))
	// }

	return hasher.Sum256(nil)
}

// ProposeMsg
type ProposeMsg struct {
	Author    NodeID
	Round     int
	B         *Block
	Signature crypto.Signature
}

func NewProposeMsg(
	Author NodeID,
	Round int,
	B *Block,
	sigService *crypto.SigService,
) (*ProposeMsg, error) {
	msg := &ProposeMsg{
		Author: Author,
		Round:  Round,
		B:      B,
	}
	if sig, err := sigService.RequestSignature(msg.Hash()); err != nil {
		return nil, err
	} else {
		msg.Signature = sig
		return msg, nil
	}
}

func (msg *ProposeMsg) Verify(committee Committee) bool {
	return msg.Signature.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *ProposeMsg) Hash() crypto.Digest {

	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.Round), 2))
	digest := msg.B.Hash()
	hasher.Add(digest[:])
	return hasher.Sum256(nil)
}

func (msg *ProposeMsg) MsgType() int {
	return ProposeMsgType
}

//ABA MSG
// type ABAVal struct {
// 	Author    NodeID
// 	round     int
// 	Round     int64
// 	InRound   int64
// 	Flag      uint8
// 	Signature crypto.Signature
// }

// func NewABAVal(Author, Leader NodeID, Epoch, Round, InRound int64, Flag uint8, sigService *crypto.SigService) (*ABAVal, error) {
// 	val := &ABAVal{
// 		Author:  Author,
// 		Round:   Round,
// 		InRound: InRound,
// 		Flag:    Flag,
// 	}
// 	sig, err := sigService.RequestSignature(val.Hash())
// 	if err != nil {
// 		return nil, err
// 	}
// 	val.Signature = sig
// 	return val, nil
// }

// func (v *ABAVal) Verify(committee Committee) bool {
// 	pub := committee.Name(v.Author)
// 	return v.Signature.Verify(pub, v.Hash())
// }

// func (v *ABAVal) Hash() crypto.Digest {
// 	hasher := crypto.NewHasher()
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(v.Author)))
// 	hasher.Add([]byte{v.Flag})
// 	return hasher.Sum256(nil)
// }

// func (v *ABAVal) MsgType() int {
// 	return ABAValType
// }

// type ABAMux struct {
// 	Author    NodeID
// 	Leader    NodeID
// 	Epoch     int64
// 	Round     int64
// 	InRound   int64
// 	Flag      uint8
// 	Signature crypto.Signature
// }

// func NewABAMux(Author, Leader NodeID, Epoch, Round, InRound int64, Flag uint8, sigService *crypto.SigService) (*ABAMux, error) {
// 	val := &ABAMux{
// 		Author:  Author,
// 		Leader:  Leader,
// 		Epoch:   Epoch,
// 		Round:   Round,
// 		InRound: InRound,
// 		Flag:    Flag,
// 	}
// 	sig, err := sigService.RequestSignature(val.Hash())
// 	if err != nil {
// 		return nil, err
// 	}
// 	val.Signature = sig
// 	return val, nil
// }

// func (v *ABAMux) Verify(committee Committee) bool {
// 	pub := committee.Name(v.Author)
// 	return v.Signature.Verify(pub, v.Hash())
// }

// func (v *ABAMux) Hash() crypto.Digest {
// 	hasher := crypto.NewHasher()
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(v.Author)))
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(v.Leader)))
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(v.Epoch)))
// 	hasher.Add([]byte{v.Flag})
// 	return hasher.Sum256(nil)
// }

// func (v *ABAMux) MsgType() int {
// 	return ABAMuxType
// }

// type CoinShare struct {
// 	Author  NodeID
// 	Leader  NodeID
// 	Epoch   int64
// 	Round   int64
// 	InRound int64
// 	Share   crypto.SignatureShare
// }

// func NewCoinShare(Author, Leader NodeID, Epoch, Round, InRound int64, sigService *crypto.SigService) (*CoinShare, error) {
// 	coin := &CoinShare{
// 		Author:  Author,
// 		Leader:  Leader,
// 		Epoch:   Epoch,
// 		Round:   Round,
// 		InRound: InRound,
// 	}
// 	sig, err := sigService.RequestTsSugnature(coin.Hash())
// 	if err != nil {
// 		return nil, err
// 	}
// 	coin.Share = sig
// 	return coin, nil
// }

// func (c *CoinShare) Verify(committee Committee) bool {
// 	_ = committee.Name(c.Author)
// 	return c.Share.Verify(c.Hash())
// }

// func (c *CoinShare) Hash() crypto.Digest {
// 	hasher := crypto.NewHasher()
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(c.Leader)))
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(c.Epoch)))
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(c.InRound)))
// 	return hasher.Sum256(nil)
// }

// func (c *CoinShare) MsgType() int {
// 	return CoinShareType
// }

// type ABAHalt struct {
// 	Author    NodeID
// 	Leader    NodeID
// 	Epoch     int64
// 	Round     int64
// 	InRound   int64
// 	Flag      uint8
// 	Signature crypto.Signature
// }

// func NewABAHalt(Author, Leader NodeID, Epoch, Round, InRound int64, Flag uint8, sigService *crypto.SigService) (*ABAHalt, error) {
// 	h := &ABAHalt{
// 		Author:  Author,
// 		Leader:  Leader,
// 		Epoch:   Epoch,
// 		Round:   Round,
// 		InRound: InRound,
// 		Flag:    Flag,
// 	}
// 	sig, err := sigService.RequestSignature(h.Hash())
// 	if err != nil {
// 		return nil, err
// 	}
// 	h.Signature = sig
// 	return h, nil
// }

// func (h *ABAHalt) Verify(committee Committee) bool {
// 	pub := committee.Name(h.Author)
// 	return h.Signature.Verify(pub, h.Hash())
// }

// func (h *ABAHalt) Hash() crypto.Digest {
// 	hasher := crypto.NewHasher()
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(h.Author)))
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(h.Leader)))
// 	hasher.Add(binary.BigEndian.AppendUint64(nil, uint64(h.Epoch)))
// 	hasher.Add([]byte{h.Flag})
// 	return hasher.Sum256(nil)
// }

// func (h *ABAHalt) MsgType() int {
// 	return ABAHaltType
// }

// ReadyMsg
type ReadyMsg struct {
	Author    NodeID
	Proposer  NodeID
	BlockHash crypto.Digest
	Round     int
	Signature crypto.Signature
}

func NewReadyMsg(
	Author NodeID,
	Proposer NodeID,
	BlockHash crypto.Digest,
	Round int,
	sigService *crypto.SigService,
) (*ReadyMsg, error) {
	msg := &ReadyMsg{
		Author:    Author,
		Proposer:  Proposer,
		BlockHash: BlockHash,
		Round:     Round,
	}
	sig, err := sigService.RequestSignature(msg.Hash())
	if err != nil {
		return nil, err
	}
	msg.Signature = sig
	return msg, nil
}

func (msg *ReadyMsg) Verify(committee Committee) bool {
	return msg.Signature.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *ReadyMsg) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.Proposer), 2))
	hasher.Add(msg.BlockHash[:])
	hasher.Add(strconv.AppendInt(nil, int64(msg.Round), 2))
	return hasher.Sum256(nil)
}

func (msg *ReadyMsg) MsgType() int {
	return ReadyType
}

// RequestBlock
type RequestBlockMsg struct {
	Author    NodeID
	MissBlock []crypto.Digest
	Signature crypto.Signature
	ReqID     int
	Ts        int64
}

func NewRequestBlock(
	Author NodeID,
	MissBlock []crypto.Digest,
	ReqID int,
	Ts int64,
	sigService *crypto.SigService,
) (*RequestBlockMsg, error) {
	msg := &RequestBlockMsg{
		Author:    Author,
		MissBlock: MissBlock,
		ReqID:     ReqID,
		Ts:        Ts,
	}
	sig, err := sigService.RequestSignature(msg.Hash())
	if err != nil {
		return nil, err
	}
	msg.Signature = sig
	return msg, nil
}

func (msg *RequestBlockMsg) Verify(committee Committee) bool {
	return msg.Signature.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *RequestBlockMsg) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.ReqID), 2))
	for _, d := range msg.MissBlock {
		hasher.Add(d[:])
	}
	return hasher.Sum256(nil)
}

func (msg *RequestBlockMsg) MsgType() int {
	return RequestBlockType
}

// ReplyBlockMsg
type ReplyBlockMsg struct {
	Author    NodeID
	Blocks    []*Block
	ReqID     int
	Signature crypto.Signature
}

func NewReplyBlockMsg(Author NodeID, B []*Block, ReqID int, sigService *crypto.SigService) (*ReplyBlockMsg, error) {
	msg := &ReplyBlockMsg{
		Author: Author,
		Blocks: B,
		ReqID:  ReqID,
	}
	sig, err := sigService.RequestSignature(msg.Hash())
	if err != nil {
		return nil, err
	}
	msg.Signature = sig
	return msg, nil
}

func (msg *ReplyBlockMsg) Verify(committee Committee) bool {
	return msg.Signature.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *ReplyBlockMsg) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.ReqID), 2))
	return hasher.Sum256(nil)
}

func (msg *ReplyBlockMsg) MsgType() int {
	return ReplyBlockType
}

type LoopBackMsg struct {
	BlockHash crypto.Digest
}

func (msg *LoopBackMsg) Hash() crypto.Digest {
	return crypto.NewHasher().Sum256(msg.BlockHash[:])
}

func (msg *LoopBackMsg) MsgType() int {
	return LoopBackType
}

const (
	ProposeMsgType int = iota
	ReadyType
	RequestBlockType
	ReplyBlockType
	LoopBackType
	TotalNums
	// ABAValType
	// ABAMuxType
	// CoinShareType
	// ABAHaltType
)

var DefaultMsgTypes = map[int]reflect.Type{
	ProposeMsgType:   reflect.TypeOf(ProposeMsg{}),
	ReadyType:        reflect.TypeOf(ReadyMsg{}),
	RequestBlockType: reflect.TypeOf(RequestBlockMsg{}),
	ReplyBlockType:   reflect.TypeOf(ReplyBlockMsg{}),
	LoopBackType:     reflect.TypeOf(LoopBackMsg{}),
	// ABAValType:     reflect.TypeOf(ABAVal{}),
	// ABAMuxType:     reflect.TypeOf(ABAMux{}),
	// CoinShareType:  reflect.TypeOf(CoinShare{}),
	// ABAHaltType:    reflect.TypeOf(ABAHalt{}),
}
