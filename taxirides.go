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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"googlemaps.github.io/maps"
)

//taxiRide json object definition extracted from yellow_tripdata_2015 json files
type taxiRide struct {
	TotalAmount          float32  `json:"total_amount,string"`
	PassengerCount       int32    `json:"passenger_count,string"`
	RateCodeID           string   `json:"RateCodeID"`
	VendorID             string   `json:"VendorID"`
	PickupLongitude      float64  `json:"pickup_longitude,string"`
	PickupLatitude       float64  `json:"pickup_latitude,string"`
	DropoffLongitude     float64  `json:"dropoff_longitude,string"`
	DropoffLatitude      float64  `json:"dropoff_latitude,string"`
	MTATax               string   `json:"mta_tax"`
	TPepPickupDatetime   rideTime `json:"tpep_pickup_datetime"`
	TPepDropoffDatetime  rideTime `json:"tpep_dropoff_datetime"`
	StoreAndFwdFlag      string   `json:"store_and_fwd_flag"`
	TipAmount            float32  `json:"tip_amount,string"`
	ImprovementSurcharge float32  `json:"improvement_surcharge,string"`
	TripDistance         string   `json:"trip_distance"`
	PaymentType          int32    `json:"payment_type,string"`
	FareAmount           float32  `json:"fare_amount,string"`
	TollsAmount          float32  `json:"tolls_amount,string"`
	Polyline             string   `json:"polyline"`
	Extra                float32  `json:"extra,string"`
}

type taxiRides struct {
	Rides []*taxiRide `json:"rides"`
}

type rideTime time.Time

func (t *rideTime) UnmarshalJSON(b []byte) error {
	v, err := parseInputTimeString(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}
	*t = rideTime(v)
	return nil
}

// Point {
// Lat,
// Long,
// Timesamp,
// Ride Id,
// Meter, // $$$
// Meter increment, // $$$
// Flag “pick up”, “drop off” or “en route”
// }
type taxiRidePoint struct {
	RideID         string  `json:"ride_id"`
	PointIdx       int     `json:"point_idx"`
	Latitude       float64 `json:"latitude"`
	Longitude      float64 `json:"longitude"`
	Timestamp      string  `json:"timestamp"`
	MeterReading   float32 `json:"meter_reading"`
	MeterIncrement float32 `json:"meter_increment"`
	RideStatus     string  `json:"ride_status"`
	PassengerCount int32   `json:"passenger_count"`
}

const (
	pickup  = "pickup"
	enroute = "enroute"
	dropoff = "dropoff"
)

func ridesFromJSONRawBytes(jsonRaw []byte) (*taxiRides, error) {
	var taxiRides taxiRides
	err := json.Unmarshal(jsonRaw, &taxiRides)
	if err != nil {
		return nil, err
	}
	return &taxiRides, nil
}

// ridePoints decodes taxiRide polyline and adds information to each point according to
// the taxiRidePoint format (a timestamp is calculated from refTime, timeOffset and speedupFactor)
// refTime is the time that represents the new dataset "start" time
// timeOffset is the time between the original dataset "start" time and the refTime
func (tr *taxiRide) ridePoints(refTime time.Time, timeOffset time.Duration, speedupFactor int) ([]*taxiRidePoint, error) {
	points := maps.DecodePolyline(tr.Polyline)

	pickupTime := time.Time(tr.TPepPickupDatetime)

	dropOffTime := time.Time(tr.TPepDropoffDatetime)

	var timeStampIncrement int64
	var meterIncrement float32
	if len(points) < 2 {
		timeStampIncrement = dropOffTime.Sub(pickupTime).Nanoseconds() / int64(speedupFactor)
		meterIncrement = tr.TotalAmount
	} else {
		timeStampIncrement = dropOffTime.Sub(pickupTime).Nanoseconds() / int64(len(points)-1) / int64(speedupFactor)
		meterIncrement = tr.TotalAmount / float32(len(points)-1)
	}

	taxiRidePoints := make([]*taxiRidePoint, len(points))

	var rideStatus = pickup
	startTimeStamp := pickupTime.Add(timeOffset)

	if speedupFactor != 1 {
		// taxi ride start time diff to refTime in realtime
		td := startTimeStamp.Sub(refTime)
		// adjust start time by speedupFactor
		startTimeStamp = startTimeStamp.Add(time.Duration(td.Nanoseconds()/int64(speedupFactor)) - td)
	}

	rID, err := newUUID()
	if err != nil {
		return nil, err
	}

	for i, p := range points {
		if i > 0 && i < len(points)-1 {
			rideStatus = enroute
		}
		if i == len(points)-1 {
			rideStatus = dropoff
		}

		taxiRidePoints[i] = &taxiRidePoint{
			RideID:         rID,
			PointIdx:       i,
			Latitude:       p.Lat,
			Longitude:      p.Lng,
			Timestamp:      startTimeStamp.Add(time.Duration(timeStampIncrement * int64(i))).Format(outputDateTimeLayout),
			MeterReading:   meterIncrement * float32(i),
			MeterIncrement: meterIncrement,
			RideStatus:     rideStatus,
			PassengerCount: tr.PassengerCount,
		}
	}
	return taxiRidePoints, nil
}

func (tr *taxiRide) valid() bool {
	pt := time.Time(tr.TPepPickupDatetime)
	dt := time.Time(tr.TPepDropoffDatetime)

	if pt.IsZero() || dt.IsZero() {
		schedDebugCounter.Add(ridesInvalidKey, 1)
		return false
	}

	// if ride is longer than 6 hours declare as invalid
	if dt.Sub(pt).Hours() > 6 {
		schedDebugCounter.Add(ridesInvalidKey, 1)
		return false
	}
	return true
}

// newUUID generates a random UUID according to RFC 4122
// taken from Go Playground
func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}
