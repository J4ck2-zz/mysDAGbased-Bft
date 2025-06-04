package consensus

import (
	"WuKong/core"
	"WuKong/crypto"
	"WuKong/logger"
	"sync"
	"sync/atomic"
)

type SPB struct {
	c         *Core
	Proposer  core.NodeID
	Epoch     int64
	Round     int64
	BlockHash atomic.Value

	vm    sync.Mutex
	Votes map[int8]int

	uvm              sync.Mutex
	unHandleVote     []*SPBVote
	unHandleProposal []*SPBProposal

	LockFlag atomic.Bool
}

func NewSPB(c *Core, epoch, round int64, proposer core.NodeID) *SPB {
	return &SPB{
		c:            c,
		Epoch:        epoch,
		Round:        round,
		Proposer:     proposer,
		unHandleVote: make([]*SPBVote, 0),
		Votes:        make(map[int8]int),
	}
}

func (s *SPB) processProposal(p *SPBProposal) {
	if p.Phase == SPB_ONE_PHASE {
		// already recieve
		if s.BlockHash.Load() != nil || s.Proposer != p.B.Author {
			return
		}

		s.BlockHash.Store(p.B.Hash())

		if vote, err := NewSPBVote(s.c.nodeID, p.Author, p.B.Hash(), s.Epoch, s.Round, p.Phase, s.c.sigService); err != nil {
			logger.Error.Printf("create spb vote message error:%v \n", err)
		} else {
			if s.c.nodeID != s.Proposer {
				s.c.transmitor.Send(s.c.nodeID, s.Proposer, vote)
			} else {
				s.c.transmitor.SMVBARecvChannel() <- vote
			}
		}

		s.uvm.Lock()
		for _, proposal := range s.unHandleProposal {
			go s.processProposal(proposal)
		}
		for _, vote := range s.unHandleVote {
			go s.processVote(vote)
		}
		s.unHandleProposal = nil
		s.unHandleVote = nil
		s.uvm.Unlock()

	} else if p.Phase == SPB_TWO_PHASE {
		if s.BlockHash.Load() == nil {
			s.uvm.Lock()
			defer s.uvm.Unlock()
			s.unHandleProposal = append(s.unHandleProposal, p)
			return
		}
		//if lock ensure SPB_ONE_PHASE has received
		s.LockFlag.Store(true)
		if vote, err := NewSPBVote(s.c.nodeID, p.Author, crypto.Digest{}, s.Epoch, s.Round, p.Phase, s.c.sigService); err != nil {
			logger.Error.Printf("create spb vote message error:%v \n", err)
		} else {
			if s.c.nodeID != s.Proposer {
				s.c.transmitor.Send(s.c.nodeID, s.Proposer, vote)
			} else {
				s.c.transmitor.SMVBARecvChannel() <- vote
			}
		}
	}
}

func (s *SPB) processVote(p *SPBVote) {
	if s.BlockHash.Load() == nil {
		s.uvm.Lock()
		s.unHandleVote = append(s.unHandleVote, p)
		s.uvm.Unlock()
		return
	}
	s.vm.Lock()
	s.Votes[p.Phase]++
	num := s.Votes[p.Phase]
	s.vm.Unlock()
	// 2f+1?
	if num == s.c.committee.HightThreshold() {
		if p.Phase == SPB_ONE_PHASE {
			if proposal, err := NewSPBProposal(
				s.c.nodeID,
				nil,
				s.Epoch,
				s.Round,
				SPB_TWO_PHASE,
				s.c.sigService,
			); err != nil {
				logger.Error.Printf("create spb proposal message error:%v \n", err)
			} else {
				s.c.transmitor.Send(s.c.nodeID, core.NONE, proposal)
				s.c.transmitor.SMVBARecvChannel() <- proposal
			}
		} else if p.Phase == SPB_TWO_PHASE {
			blockHash := s.BlockHash.Load().(crypto.Digest)
			if finish, err := NewFinish(s.c.nodeID, blockHash, s.Epoch, s.Round, s.c.sigService); err != nil {
				logger.Error.Printf("create finish message error:%v \n", err)
			} else {
				s.c.transmitor.Send(s.c.nodeID, core.NONE, finish)
				s.c.transmitor.SMVBARecvChannel() <- finish
			}
		}
	}
}

func (s *SPB) IsLock() bool {
	return s.LockFlag.Load()
}

func (s *SPB) GetBlockHash() any {
	return s.BlockHash.Load()
}
