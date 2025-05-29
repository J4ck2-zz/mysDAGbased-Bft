package consensus

import (
	"WuKong/core"
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/store"
)

// func (c *Core) messgaeFilter(epoch int64) bool {
// 	return epoch < c.Epoch
// }

func (c *Core) storesMVBAValue(block *SmvbaValue) error {
	key := block.Hash()
	value, err := block.Encode()
	if err != nil {
		return err
	}
	return c.store.Write(key[:], value)
}

func (c *Core) getsMVBAValue(digest crypto.Digest) (*SmvbaValue, error) {
	value, err := c.store.Read(digest[:])

	if err == store.ErrNotFoundKey {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	b := &SmvbaValue{}
	if err := b.Decode(value); err != nil {
		return nil, err
	}
	return b, err
}

func (c *Core) handlesMVBAStart(m *SMVBAid) error {

	value := &SmvbaValue{
		Author:     c.nodeID,
		Epoch:      int64(m.epoch),
		Blockhashs: m.blockhashs,
	}

	proposal, _ := NewSPBProposal(c.nodeID, value, int64(m.epoch), 0, SPB_ONE_PHASE, c.sigService)
	c.transmitor.Send(c.nodeID, core.NONE, proposal)
	c.transmitor.RecvChannel() <- proposal

	return nil
}

func (c *Core) getSpbInstance(epoch, round int64, node core.NodeID) *SPB {
	rItems, ok := c.SPbInstances[epoch]
	if !ok {
		rItems = make(map[int64]map[core.NodeID]*SPB)
		c.SPbInstances[epoch] = rItems
	}
	instances, ok := rItems[round]
	if !ok {
		instances = make(map[core.NodeID]*SPB)
		rItems[round] = instances
	}
	instance, ok := instances[node]
	if !ok {
		instance = NewSPB(c, epoch, round, node)
		instances[node] = instance
	}

	return instance
}

func (c *Core) hasFinish(epoch, round int64, node core.NodeID) (bool, crypto.Digest) {
	if items, ok := c.FinishFlags[epoch]; !ok {
		return false, crypto.Digest{}
	} else {
		if item, ok := items[round]; !ok {
			return false, crypto.Digest{}
		} else {
			d, ok := item[node]
			return ok, d
		}
	}
}

func (c *Core) hasReady(epoch, round int64) bool {
	if items, ok := c.ReadyFlags[epoch]; !ok {
		c.ReadyFlags[epoch] = make(map[int64]struct{})
		return false
	} else {
		_, ok = items[round]
		return ok
	}
}

func (c *Core) hasDone(epoch, round int64) bool {
	if items, ok := c.DoneFlags[epoch]; !ok {
		c.DoneFlags[epoch] = make(map[int64]struct{})
		return false
	} else {
		_, ok = items[round]
		return ok
	}
}

/*********************************** Protocol Start***************************************/
func (c *Core) handleSpbProposal(p *SPBProposal) error {
	logger.Debug.Printf("Processing SPBProposal proposer %d epoch %d round %d phase %d\n", p.Author, p.Epoch, p.Round, p.Phase)

	//ensure all block is received
	if p.Phase == SPB_ONE_PHASE {
		if _, ok := c.HaltFlags[p.Epoch]; ok {
			if leader := c.Elector.Leader(p.Epoch, p.Round); leader == p.Author {
				//选出leader怎么做

			}
		}
	}

	//discard message
	// if c.messgaeFilter(p.Epoch) {
	// 	return nil
	// }

	//Store Block at first time
	if p.Phase == SPB_ONE_PHASE {
		if err := c.storesMVBAValue(p.B); err != nil {
			logger.Error.Printf("Store Value error: %v\n", err)
			return err
		}
		logger.Debug.Printf("store epoch %d hash %x\n", p.Epoch, p.B.Hash())
	}

	spb := c.getSpbInstance(p.Epoch, p.Round, p.Author)
	go spb.processProposal(p)

	return nil
}

func (c *Core) handleSpbVote(v *SPBVote) error {
	logger.Debug.Printf("Processing SPBVote proposer %d epoch %d round %d phase %d\n", v.Proposer, v.Epoch, v.Round, v.Phase)

	spb := c.getSpbInstance(v.Epoch, v.Round, v.Proposer)
	go spb.processVote(v)

	return nil
}

func (c *Core) handleFinish(f *Finish) error {
	logger.Debug.Printf("Processing Finish epoch %d round %d hash %x\n", f.Epoch, f.Round, f.BlockHash)

	//discard message
	// if c.messgaeFilter(f.Epoch) {
	// 	return nil
	// }
	if flag, err := c.Aggreator.AddFinishVote(f); err != nil {
		return err
	} else {
		rF, ok := c.FinishFlags[f.Epoch]
		if !ok {
			rF = make(map[int64]map[core.NodeID]crypto.Digest)
			c.FinishFlags[f.Epoch] = rF
		}
		nF, ok := rF[f.Round]
		if !ok {
			nF = make(map[core.NodeID]crypto.Digest)
			rF[f.Round] = nF
		}
		nF[f.Author] = f.BlockHash
		if flag {
			return c.invokeDoneAndShare(f.Epoch, f.Round)
		}
	}

	return nil
}

func (c *Core) invokeDoneAndShare(epoch, round int64) error {
	logger.Debug.Printf("Processing invoke Done and Share epoch %d,round %d\n", epoch, round)

	if !c.hasDone(epoch, round) {

		done, _ := NewDone(c.nodeID, epoch, round, c.sigService)
		share, _ := NewElectShare(c.nodeID, epoch, round, c.sigService)

		c.transmitor.Send(c.nodeID, core.NONE, done)
		c.transmitor.Send(c.nodeID, core.NONE, share)
		c.transmitor.RecvChannel() <- done
		c.transmitor.RecvChannel() <- share

		items, ok := c.DoneFlags[epoch]
		if !ok {
			items = make(map[int64]struct{})
			c.DoneFlags[epoch] = items
		}
		items[round] = struct{}{}
	}

	return nil
}

func (c *Core) handleDone(d *Done) error {
	logger.Debug.Printf("Processing Done epoch %d round %d\n", d.Epoch, d.Round)

	//discard message
	// if c.messgaeFilter(d.Epoch) {
	// 	return nil
	// }

	if flag, err := c.Aggreator.AddDoneVote(d); err != nil {
		return err
	} else if flag == DONE_LOW_FLAG {
		return c.invokeDoneAndShare(d.Epoch, d.Round)
	} else if flag == DONE_HIGH_FLAG {
		items, ok := c.ReadyFlags[d.Epoch]
		if !ok {
			items = make(map[int64]struct{})
			c.ReadyFlags[d.Epoch] = items
		}
		items[d.Round] = struct{}{}
		return c.processLeader(d.Epoch, d.Round)
	}

	return nil
}

func (c *Core) handleElectShare(share *ElectShare) error {
	logger.Debug.Printf("Processing ElectShare epoch %d round %d\n", share.Epoch, share.Round)

	//discard message
	// if c.messgaeFilter(share.Epoch) {
	// 	return nil
	// }

	if leader, err := c.Elector.AddShareVote(share); err != nil {
		return err
	} else if leader != core.NONE {
		c.processLeader(share.Epoch, share.Round)
	}

	return nil
}

func (c *Core) processLeader(epoch, round int64) error {
	logger.Debug.Printf("Processing Leader epoch %d round %d Leader %d\n", epoch, round, c.Elector.Leader(epoch, round))
	if c.hasReady(epoch, round) {
		if leader := c.Elector.Leader(epoch, round); leader != core.NONE {
			if ok, d := c.hasFinish(epoch, round, leader); ok {
				//send halt
				halt, _ := NewHalt(c.nodeID, leader, d, epoch, round, c.sigService)
				c.transmitor.Send(c.nodeID, core.NONE, halt)
				c.transmitor.RecvChannel() <- halt
			} else {
				//send preVote
				var preVote *Prevote
				if spb := c.getSpbInstance(epoch, round, leader); spb.IsLock() {
					if blockHash, ok := spb.GetBlockHash().(crypto.Digest); !ok {
						panic("block hash is nil")
					} else {
						preVote, _ = NewPrevote(c.nodeID, leader, epoch, round, VOTE_FLAG_YES, blockHash, c.sigService)
					}
				} else {
					preVote, _ = NewPrevote(c.nodeID, leader, epoch, round, VOTE_FLAG_NO, crypto.Digest{}, c.sigService)
				}
				c.transmitor.Send(c.nodeID, core.NONE, preVote)
				c.transmitor.RecvChannel() <- preVote
			}
		}
	}

	return nil
}

func (c *Core) handlePrevote(pv *Prevote) error {
	logger.Debug.Printf("Processing Prevote epoch %d round %d hash %x\n", pv.Epoch, pv.Round, pv.BlockHash)

	if flag, err := c.Aggreator.AddPreVote(pv); err != nil {
		return err
	} else if flag == ACTION_NO {
		vote, _ := NewFinVote(c.nodeID, pv.Leader, pv.Epoch, pv.Round, VOTE_FLAG_NO, pv.BlockHash, c.sigService)
		c.transmitor.Send(c.nodeID, core.NONE, vote)
		c.transmitor.RecvChannel() <- vote
	} else if flag == ACTION_YES {
		vote, _ := NewFinVote(c.nodeID, pv.Leader, pv.Epoch, pv.Round, VOTE_FLAG_YES, pv.BlockHash, c.sigService)
		c.transmitor.Send(c.nodeID, core.NONE, vote)
		c.transmitor.RecvChannel() <- vote
	}

	return nil
}

func (c *Core) handleFinvote(fv *FinVote) error {
	logger.Debug.Printf("Processing FinVote epoch %d round %d hash %x\n", fv.Epoch, fv.Round, fv.BlockHash)

	if flag, err := c.Aggreator.AddFinVote(fv); err != nil {
		return err
	} else if flag == ACTION_YES {
		return c.advanceNextRound(fv.Epoch, fv.Round, flag, fv.BlockHash)
	} else if flag == ACTION_NO {
		return c.advanceNextRound(fv.Epoch, fv.Round, flag, crypto.Digest{})
	} else if flag == ACTION_COMMIT {
		halt, _ := NewHalt(c.nodeID, fv.Leader, fv.BlockHash, fv.Epoch, fv.Round, c.sigService)
		c.transmitor.Send(c.nodeID, core.NONE, halt)
		c.transmitor.RecvChannel() <- halt
	}

	return nil
}

func (c *Core) advanceNextRound(epoch, round int64, flag int8, blockHash crypto.Digest) error {
	logger.Debug.Printf("Processing next round [epoch %d round %d]\n", epoch, round)

	if flag == ACTION_NO { //next round block self
		if inte := c.getSpbInstance(epoch, round, c.nodeID).GetBlockHash(); inte != nil {
			blockHash = inte.(crypto.Digest)
		}
	}

	var proposal *SPBProposal
	if block, err := c.getsMVBAValue(blockHash); err != nil {
		return err
	} else if block != nil {
		proposal, _ = NewSPBProposal(c.nodeID, block, epoch, round+1, SPB_ONE_PHASE, c.sigService)
	} else {
		//重新提块
		proposal, _ = NewSPBProposal(c.nodeID, nil, epoch, round+1, SPB_ONE_PHASE, c.sigService)
	}
	c.transmitor.Send(c.nodeID, core.NONE, proposal)
	c.transmitor.RecvChannel() <- proposal

	return nil
}

func (c *Core) handleHalt(h *Halt) error {
	logger.Debug.Printf("Processing Halt epoch %d\n", h.Epoch)

	//Check leader
	var err error
	if _, ok := c.HaltFlags[h.Epoch]; !ok {
		c.Elector.SetLeader(h.Epoch, h.Round, h.Leader)

		//smvba输出
		err = c.handlesMVBAOutput(h.Epoch, h.BlockHash)
		c.HaltFlags[h.Epoch] = struct{}{}
	}

	return err
}

func (c *Core) handlesMVBAOutput(epoch int64, blockHash crypto.Digest) error {
	logger.Debug.Printf("Processing Ouput epoch %d hash %x\n", epoch, blockHash)
	if b, err := c.getsMVBAValue(blockHash); err != nil {
		logger.Debug.Printf(" handlesMVBAOutput error")
		return err
	} else if b != nil {
		//smvba输出
		var zero crypto.Digest
		for ix, digest := range b.Blockhashs {
			if digest != zero {
				logger.Debug.Printf("sMVBA Ouput tocommit epoch %d node &d \n", epoch, ix)
				c.commitor.notifycommit <- &commitMsg{
					round:  int(b.Epoch),
					node:   core.NodeID(ix),
					ptern:  toCommit,
					digest: digest,
				}
			} else {
				logger.Debug.Printf("sMVBA Ouput toskip epoch %d node &d \n", epoch, ix)
				c.commitor.notifycommit <- &commitMsg{
					round:  int(b.Epoch),
					node:   core.NodeID(ix),
					ptern:  toskip,
					digest: crypto.Digest{},
				}
			}

		}

	}
	// else {
	// 	logger.Debug.Printf("Processing retriever epoch %d \n", epoch)
	// }

	return nil
}
