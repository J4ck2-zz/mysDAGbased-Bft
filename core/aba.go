package core

import (
	"WuKong/logger"
	"sync"
	"sync/atomic"
)

const (
	ABA_INVOKE = iota
	ABA_HALT
)

type ABABack struct {
	Typ     int
	ExRound int
	Slot    NodeID
	InRound int
	Flag    uint8
}

type ABA struct {
	c           *Core
	ExRound     int
	Slot        NodeID
	yesUsed     map[int]map[NodeID]struct{}
	noUsed      map[int]map[NodeID]struct{}
	initMutex   sync.RWMutex
	initYesCnt  int
	initNoCnt   int
	valMutex    sync.Mutex
	valYesCnt   map[int]int
	valNoCnt    map[int]int
	flagMutex   sync.Mutex
	muxFlag     map[int]struct{}
	muxFinFlag  map[int]struct{}
	yesFlag     map[int]struct{}
	noFlag      map[int]struct{}
	muxMutex    sync.Mutex
	muxYesCnt   map[int]int
	muxNoCnt    map[int]int
	halt        atomic.Bool
	abaCallBack chan *ABABack
}

func NewABA(c *Core, ExRound int, Slot NodeID, abaCallBack chan *ABABack) *ABA {
	return &ABA{
		c:           c,
		ExRound:     ExRound,
		Slot:        Slot,
		yesUsed:     make(map[int]map[NodeID]struct{}),
		noUsed:      make(map[int]map[NodeID]struct{}),
		initYesCnt:  0,
		initNoCnt:   0,
		valYesCnt:   map[int]int{},
		valNoCnt:    map[int]int{},
		muxFlag:     map[int]struct{}{},
		muxFinFlag:  map[int]struct{}{},
		yesFlag:     map[int]struct{}{},
		noFlag:      map[int]struct{}{},
		muxYesCnt:   map[int]int{},
		muxNoCnt:    map[int]int{},
		abaCallBack: abaCallBack,
	}
}

func (aba *ABA) isUsed(val *ABAVal, used map[int]map[NodeID]struct{}) bool {
	item, ok := used[val.InRound]
	if !ok {
		return false
	}
	_, ok = item[val.Author]
	return ok

}

func (aba *ABA) ProcessABAVal(val *ABAVal) {
	if aba.halt.Load() {
		return
	}
	var cnt int
	aba.valMutex.Lock()
	if val.LocalState == FLAG_YES {
		if val.Flag == FLAG_YES {
			aba.initMutex.Lock()
			aba.initYesCnt++
			yescnt := aba.initYesCnt
			aba.initMutex.Unlock()
			if yescnt == aba.c.committee.Size() {
				logger.Debug.Printf("fastpath aba round %d Slot %d flag %d\n", val.Round, val.Slot, FLAG_YES)
				halt, _ := NewABAHalt(aba.c.nodeID, aba.ExRound, aba.Slot, val.InRound, FLAG_YES, aba.c.sigService)
				aba.c.transmitor.Send(aba.c.nodeID, NONE, halt)
				aba.c.transmitor.RecvChannel() <- halt
				return
			}
		}
		if val.Flag == FLAG_NO {
			aba.initMutex.Lock()
			aba.initNoCnt++
			nocnt := aba.initNoCnt
			aba.initMutex.Unlock()
			if nocnt == aba.c.committee.Size() {
				logger.Debug.Printf("fastpath aba round %d Slot %d flag %d\n", val.Round, val.Slot, FLAG_NO)
				halt, _ := NewABAHalt(aba.c.nodeID, aba.ExRound, aba.Slot, val.InRound, FLAG_NO, aba.c.sigService)
				aba.c.transmitor.Send(aba.c.nodeID, NONE, halt)
				aba.c.transmitor.RecvChannel() <- halt
				return
			}
		}
	}

	if val.Flag == FLAG_NO {
		if !aba.isUsed(val, aba.noUsed) {
			aba.valNoCnt[val.InRound]++
			cnt = aba.valNoCnt[val.InRound]
			flags, ok := aba.noUsed[val.InRound]
			if !ok {
				flags = make(map[NodeID]struct{})
				aba.noUsed[val.InRound] = flags
			}
			_, ok = flags[val.Slot]
			if !ok {
				flags[val.Slot] = struct{}{}
			}
		}
	} else if val.Flag == FLAG_YES {
		if !aba.isUsed(val, aba.yesUsed) {
			aba.valYesCnt[val.InRound]++
			cnt = aba.valYesCnt[val.InRound]
			flags, ok := aba.yesUsed[val.InRound]
			if !ok {
				flags = make(map[NodeID]struct{})
				aba.yesUsed[val.InRound] = flags
			}
			_, ok = flags[val.Slot]
			if !ok {
				flags[val.Slot] = struct{}{}
			}
		}
	}
	aba.valMutex.Unlock()

	if cnt == aba.c.committee.LowThreshold() {
		aba.abaCallBack <- &ABABack{
			Typ:     ABA_INVOKE,
			ExRound: aba.ExRound,
			Slot:    aba.Slot,
			InRound: val.InRound,
			Flag:    val.Flag,
		}
	} else if cnt == aba.c.committee.HightThreshold() {
		aba.flagMutex.Lock()
		defer aba.flagMutex.Unlock()
		if _, ok := aba.muxFlag[val.InRound]; !ok {
			aba.muxFlag[val.InRound] = struct{}{}
			mux, _ := NewABAMux(aba.c.nodeID, val.Round, val.Slot, val.InRound, val.Flag, aba.c.sigService)
			aba.c.transmitor.Send(aba.c.nodeID, NONE, mux)
			aba.c.transmitor.RecvChannel() <- mux
		}
	}

}

func (aba *ABA) ProcessABAMux(mux *ABAMux) {
	if aba.halt.Load() {
		return
	}
	aba.flagMutex.Lock()
	if _, ok := aba.muxFinFlag[mux.InRound]; ok {
		aba.flagMutex.Unlock()
		return
	}
	aba.flagMutex.Unlock()

	var muxYescnt, muxNocnt int
	aba.muxMutex.Lock()
	if mux.Flag == FLAG_NO {
		aba.muxNoCnt[mux.InRound]++
	} else if mux.Flag == FLAG_YES {
		aba.muxYesCnt[mux.InRound]++
	}
	muxYescnt, muxNocnt = aba.muxYesCnt[mux.InRound], aba.muxNoCnt[mux.InRound]
	aba.muxMutex.Unlock()

	var valYescnt, valNocnt int
	aba.valMutex.Lock()
	valYescnt, valNocnt = aba.valYesCnt[mux.InRound], aba.valNoCnt[mux.InRound]
	aba.valMutex.Unlock()

	var flag bool
	th := aba.c.committee.HightThreshold()

	aba.flagMutex.Lock()
	if _, ok := aba.muxFinFlag[mux.InRound]; ok { //double check
		aba.flagMutex.Unlock()
		return
	}
	if muxYescnt+muxNocnt >= th {
		if valYescnt >= th && valNocnt >= th {
			if muxYescnt > 0 {
				aba.yesFlag[mux.InRound] = struct{}{}
				aba.muxFinFlag[mux.InRound] = struct{}{}
				flag = true
			}
			if muxNocnt > 0 {
				aba.noFlag[mux.InRound] = struct{}{}
				aba.muxFinFlag[mux.InRound] = struct{}{}
				flag = true
			}
		} else if valYescnt >= th && muxYescnt >= th {
			aba.yesFlag[mux.InRound] = struct{}{}
			aba.muxFinFlag[mux.InRound] = struct{}{}
			flag = true
		} else if valNocnt >= th && muxNocnt >= th {
			aba.noFlag[mux.InRound] = struct{}{}
			aba.muxFinFlag[mux.InRound] = struct{}{}
			flag = true
		}
	}
	aba.flagMutex.Unlock()

	if flag { //only once call
		coinShare, _ := NewCoinShare(aba.c.nodeID, mux.Round, mux.Slot, mux.InRound, aba.c.sigService)
		aba.c.transmitor.Send(aba.c.nodeID, NONE, coinShare)
		aba.c.transmitor.RecvChannel() <- coinShare
	}

}

func (aba *ABA) ProcessCoin(inRound int, coin uint8) {
	aba.flagMutex.Lock()
	defer aba.flagMutex.Unlock()
	_, okYes := aba.yesFlag[inRound]
	_, okNo := aba.noFlag[inRound]

	if (okYes && okNo) || (!okYes && !okNo) { //next round with coin
		abaVal, _ := NewABAVal(aba.c.nodeID, aba.ExRound, aba.Slot, inRound+1, coin, aba.c.sigService, FLAG_NO)
		aba.c.transmitor.Send(aba.c.nodeID, NONE, abaVal)
		aba.c.transmitor.RecvChannel() <- abaVal
	} else if (okYes && coin == FLAG_YES) || (okNo && coin == FLAG_NO) {
		halt, _ := NewABAHalt(aba.c.nodeID, aba.ExRound, aba.Slot, inRound, coin, aba.c.sigService)
		aba.c.transmitor.Send(aba.c.nodeID, NONE, halt)
		aba.c.transmitor.RecvChannel() <- halt
	} else { // next round with self
		var abaVal *ABAVal
		if okYes {
			abaVal, _ = NewABAVal(aba.c.nodeID, aba.ExRound, aba.Slot, inRound+1, FLAG_YES, aba.c.sigService, FLAG_NO)
		} else if okNo {
			abaVal, _ = NewABAVal(aba.c.nodeID, aba.ExRound, aba.Slot, inRound+1, FLAG_NO, aba.c.sigService, FLAG_NO)
		}
		aba.c.transmitor.Send(aba.c.nodeID, NONE, abaVal)
		aba.c.transmitor.RecvChannel() <- abaVal
	}
}

func (aba *ABA) ProcessHalt(halt *ABAHalt) {
	aba.flagMutex.Lock()
	defer aba.flagMutex.Unlock()
	if aba.halt.Load() {
		return
	}
	aba.halt.Store(true)
	temp, _ := NewABAHalt(aba.c.nodeID, aba.ExRound, aba.Slot, halt.InRound, halt.Flag, aba.c.sigService)
	aba.c.transmitor.Send(aba.c.nodeID, NONE, temp)
	aba.c.transmitor.RecvChannel() <- temp

	aba.abaCallBack <- &ABABack{
		Typ:     ABA_HALT,
		ExRound: halt.Round,
		Slot:    halt.Slot,
		InRound: halt.InRound,
		Flag:    halt.Flag,
	}
}
