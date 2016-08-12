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
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

// TaxiRidePointPusher is responsible to push Points of a taxi ride according to their timestamps to pubsub
type ridePointScheduler struct {
	taxiRide      *taxiRide
	refTime       time.Time
	timeOffset    time.Duration
	speedupFactor int
	points        []*taxiRidePoint
	idxCount      int
	debugLog      debugging
}

func (rps *ridePointScheduler) run(ch chan<- *pubsub.Message) {
	if rps.debugLog {
		defer func() {

			schedDebugCounter.Add(pointsFailedKey, int64(len(rps.points)))
			schedDebugCounter.Add(ridesProcessedKey, 1)

		}()
	}

	pt := time.Time(rps.taxiRide.TPepPickupDatetime)

	// real time passed since refTime
	rtd := time.Now().Sub(rps.refTime)
	// time duration from reftime with speedup considered
	d := int64(pt.Add(rps.timeOffset).Sub(rps.refTime).Nanoseconds() / int64(rps.speedupFactor))
	// delay expanding of taxi ride polyline to points till ride starts
	time.Sleep(time.Duration(d - rtd.Nanoseconds()))

	var err error
	rps.points, err = rps.taxiRide.ridePoints(rps.refTime, rps.timeOffset, rps.speedupFactor)
	if err != nil {
		log.Printf("Unable to generate taxi ride points: %v", err)
		return
	}

	if rps.debugLog {
		schedDebugCounter.Add(ridesLoadedKey, 1)
		schedDebugCounter.Add(pointsLoadedKey, int64(len(rps.points)))
	}

	for {
		// if no more point in list exit
		if len(rps.points) == 0 {
			return
		}

		// calculate time to wait till pushing next ride point to pubsub
		timestamp, err := parseOutputTimeString(rps.points[0].Timestamp)
		if err != nil {
			log.Printf("Error parsing timestamp of taxi ride point. RideID: %v, Point idx: %v, Error: %v",
				rps.points[0].RideID, rps.idxCount, err)
			return
		}
		time.Sleep(timestamp.Sub(time.Now()))

		if rps.debugLog {
			schedDebugCounter.Add(pointsScheduledKey, 1)
		}

		pointJSON, err := json.Marshal(rps.points[0])
		if err != nil {
			log.Printf("Error creating json for taxiRide: %v", err)
			return
		}

		// send message on publish channel
		attributes := map[string]string{"ts": rps.points[0].Timestamp}
		select {
		case ch <- &pubsub.Message{Data: pointJSON, Attributes: attributes}:
			break
		default:
			// rps.debugLog.Println("Channel to publishing message is full. Discarding point.")
			if rps.debugLog {
				schedDebugCounter.Add(pointsFailedKey, 1)
			}
		}
		rps.idxCount++
		// remove handled point from list
		rps.points = rps.points[1:]
	}
}
