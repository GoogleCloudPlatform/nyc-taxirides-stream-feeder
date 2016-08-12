/*
Copyright 2018 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"expvar"
	"log"
	"math"
	"strconv"
	"time"
)

type debugging bool

var (
	pubDebugCounter   = expvar.NewMap("publisherCounters")
	schedDebugCounter = expvar.NewMap("schedulerCounters")
	debugStatsFormat  = "%15v %12v %9v %4v %16v %8v %11v %8v %8v %10v %10v"
)

const (
	ridesLoadedKey     = "ridesloaded"
	ridesProcessedKey  = "ridesprocessed"
	ridesInvalidKey    = "ridesinvalid"
	pointsLoadedKey    = "pointsloaded"
	pointsScheduledKey = "pointsscheduled"
	pointsFailedKey    = "pointsfailed"
	sentMsgsKey        = "sentmessages"
	failedMsgsKey      = "failedmessages"
	publishBacklogKey  = "publishbacklog"
)

// debugging log
func (d debugging) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}

func (d debugging) Println(args ...interface{}) {
	if d {
		log.Println(args...)
	}
}

func (d *debugging) startDebugHeaderPrinter() {
	d.Printf(debugStatsFormat, "Rides# queued", "processed", "invalid", "qps", "Points# total", "queued", "scheduled", "failed", "backlog", "Sched QPS", "Pub QPS")
	time.AfterFunc(time.Second*10, d.startDebugHeaderPrinter)
}

func (d *debugging) printDebugStats() {
	if !(*d) {
		return
	}

	// print debug stats table header
	d.startDebugHeaderPrinter()

	lastRidesProcessed := 0
	lastPointsScheduled := 0
	lastMessagesSent := 0
	lastStatisticsUpdateTimestamp := time.Now()
	go func() {
		for {
			time.Sleep(time.Second)
			rl := ridesLoaded()
			rp := ridesProcessed()
			rQueue := rl - rp
			ri := ridesInvalid()
			pl := pointsLoaded()
			pf := pointsFailed() + messagesFailed()
			ps := pointsScheduled()
			bl := publishBacklog()
			ms := messagesSent()
			pQueue := pl - ms - pf

			// d.Printf("Rides# queue: %v processed: %v invalid: %v qps: %v; Points# total: %v, queue: %v, scheduled: %v, failed: %v backlog: %v; Sched QPS: %v, Pub QPS: %v",
			d.Printf(debugStatsFormat, rQueue, rp, ri,
				math.Ceil(float64(rp-lastRidesProcessed)/time.Now().Sub(lastStatisticsUpdateTimestamp).Seconds()),
				pl, pQueue, ps, pf, bl,
				math.Ceil(float64(ps-lastPointsScheduled)/time.Now().Sub(lastStatisticsUpdateTimestamp).Seconds()),
				math.Ceil(float64(ms-lastMessagesSent)/time.Now().Sub(lastStatisticsUpdateTimestamp).Seconds()))
			lastStatisticsUpdateTimestamp = time.Now()
			lastRidesProcessed = rp
			lastPointsScheduled = ps
			lastMessagesSent = ms
		}
	}()
}

func ridesLoaded() int {
	rl := schedDebugCounter.Get(ridesLoadedKey)
	if rl == nil {
		return 0
	}
	rli, err := strconv.Atoi(rl.String())
	if err != nil {
		log.Printf("Can't get rides loaded count. %v", err)
		return 0
	}
	return rli
}

func ridesProcessed() int {
	rp := schedDebugCounter.Get(ridesProcessedKey)
	if rp == nil {
		return 0
	}
	rpi, err := strconv.Atoi(rp.String())
	if err != nil {
		log.Printf("Can't get rides processed count. %v", err)
		return 0
	}
	return rpi
}

func ridesInvalid() int {
	ri := schedDebugCounter.Get(ridesInvalidKey)
	if ri == nil {
		return 0
	}
	rii, err := strconv.Atoi(ri.String())
	if err != nil {
		log.Printf("Can't get rides invalid count. %v", err)
		return 0
	}
	return rii
}

func pointsLoaded() int {
	pl := schedDebugCounter.Get(pointsLoadedKey)
	if pl == nil {
		return 0
	}
	pli, err := strconv.Atoi(pl.String())
	if err != nil {
		log.Printf("Can't get points loaded count. %v", err)
		return 0
	}
	return pli
}

func pointsScheduled() int {
	pp := schedDebugCounter.Get(pointsScheduledKey)
	if pp == nil {
		return 0
	}
	ppi, err := strconv.Atoi(pp.String())
	if err != nil {
		log.Printf("Can't get points scheduled count. %v", err)
		return 0
	}
	return ppi
}

func pointsFailed() int {
	pf := schedDebugCounter.Get(pointsFailedKey)
	if pf == nil {
		return 0
	}
	pfi, err := strconv.Atoi(pf.String())
	if err != nil {
		log.Printf("Can't get points failed count. %v", err)
		return 0
	}
	return pfi
}

func messagesSent() int {
	ms := pubDebugCounter.Get(sentMsgsKey)
	if ms == nil {
		return 0
	}
	msi, err := strconv.Atoi(ms.String())
	if err != nil {
		log.Printf("Can't get messages sent count. %v", err)
		return 0
	}
	return msi
}

func messagesFailed() int {
	mf := pubDebugCounter.Get(failedMsgsKey)
	if mf == nil {
		return 0
	}
	mfi, err := strconv.Atoi(mf.String())
	if err != nil {
		log.Printf("Can't get messages failed count. %v", err)
		return 0
	}
	return mfi
}

func publishBacklog() int {
	bl := pubDebugCounter.Get(publishBacklogKey)
	if bl == nil {
		return 0
	}
	bli, err := strconv.Atoi(bl.String())
	if err != nil {
		log.Printf("Can't get backlog count. %v", err)
		return 0
	}
	return bli
}
