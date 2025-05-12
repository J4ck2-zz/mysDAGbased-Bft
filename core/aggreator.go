package core

import (
	"WuKong/crypto"
	"WuKong/logger"
)

const RANDOM_LEN = 7

type Aggreator struct {
	committee  Committee
	sigService *crypto.SigService
	coins      map[int]map[NodeID]map[int]*CoinAggreator //round-Slot-inround
}

func NewAggreator(committee Committee, sigService *crypto.SigService) *Aggreator {
	a := &Aggreator{
		committee:  committee,
		sigService: sigService,

		coins: make(map[int]map[NodeID]map[int]*CoinAggreator),
	}

	return a
}

func (a *Aggreator) addCoinShare(coinShare *CoinShare) (bool, uint8, error) {
	items, ok := a.coins[coinShare.Round]
	if !ok {
		items = make(map[NodeID]map[int]*CoinAggreator)
		a.coins[coinShare.Round] = items
	}
	item, ok := items[coinShare.Slot]
	if !ok {
		item = make(map[int]*CoinAggreator)
		items[coinShare.Slot] = item
	}
	instance, ok := item[coinShare.InRound]
	if !ok {
		instance = NewCoinAggreator()
		items[coinShare.Slot][coinShare.InRound] = instance
	}

	return instance.append(a.committee, a.sigService, coinShare)
}

type CoinAggreator struct {
	Used   map[NodeID]struct{}
	Shares []crypto.SignatureShare
}

func NewCoinAggreator() *CoinAggreator {
	return &CoinAggreator{
		Used:   make(map[NodeID]struct{}),
		Shares: make([]crypto.SignatureShare, 0),
	}
}

func (c *CoinAggreator) append(committee Committee, sigService *crypto.SigService, share *CoinShare) (bool, uint8, error) {
	if _, ok := c.Used[share.Author]; ok {
		return false, 0, ErrOneMoreMessage(share.MsgType(), share.Round, share.Slot, share.Author)
	}
	c.Shares = append(c.Shares, share.Share)
	if len(c.Shares) == committee.HightThreshold() {
		var seed uint64 = 0
		data, err := crypto.CombineIntactTSPartial(c.Shares, sigService.ShareKey, share.Hash())
		if err != nil {
			logger.Error.Printf("share author %d round %d Slot %d inround %d\n", share.Author, share.Round, share.Slot, share.InRound)
			logger.Error.Printf("Combine signature error: %v\n", err)
			return false, 0, err
		}
		for i := 0; i < len(data) && i < RANDOM_LEN; i++ {
			seed = seed<<8 + uint64(data[i])
		}
		return true, uint8(seed % 2), nil
	}

	return false, 0, nil
}
