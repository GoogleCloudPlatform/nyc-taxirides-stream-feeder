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
	"io"
	"log"
	"testing"
	"time"
)

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }

var jsonRaw = "{\"rides\":[{\"total_amount\": \"6.65\", \"passenger_count\": \"3\", \"RateCodeID\": \"1\", \"dropoff_longitude\": \"-73.992393493652344\", \"VendorID\": \"2\", \"pickup_latitude\": \"40.757595062255859\", \"mta_tax\": \"0.5\", \"tpep_dropoff_datetime\": \"2015-01-04 20:03:02\", \"store_and_fwd_flag\": \"N\", \"pickup_longitude\": \"-73.989242553710938\", \"tip_amount\": \"1.35\", \"dropoff_latitude\": \"40.7625732421875\", \"improvement_surcharge\": \"0.3\", \"trip_distance\": \".50\", \"payment_type\": \"1\", \"tpep_pickup_datetime\": \"2015-01-04 20:00:40\", \"fare_amount\": \"4\", \"tolls_amount\": \"0\", \"polyline\": \"qnwwFbarbMOKOKOKQKOKOKOKOKQKOKOKOKOKOKQKOKOKOKOKQKOKOMQKOKOKQKOKOKQKOMOKQKOKGPGRGPGPGPERGPGPGRGPGPGPGRGPGPERGPGPGRGPGPGPGRGPGPERGPGPGPGRGPGRGPGRGPGRGPGRGPGRGPGRGRGPGRGPGRGPGRGPGRGPGR\", \"extra\": \"0.5\"}]}"

func TestRidesFromJSONRawBytes(t *testing.T) {

	trs, err := ridesFromJSONRawBytes([]byte(jsonRaw))
	if err != nil {
		t.Error(err)
	}

	for _, tr := range trs.Rides {
		GetRidePointsTest(t, tr)
	}
}

func GetRidePointsTest(t *testing.T, tr *taxiRide) {
	dsst, err := parseInputTimeString("2015-01-04 20:00:00")
	if err != nil {
		log.Fatalf("Unable to parse dataset base time %v: %v", "2015-01-04 20:00:00", err)
	}
	now := time.Now()
	to := now.Sub(dsst)

	// get ridepoints with speedup factor 1
	trps, err := tr.ridePoints(now, to, 1)
	if err != nil {
		t.Error(err)
	}

	ptime := time.Time(tr.TPepPickupDatetime)

	dtime := time.Time(tr.TPepDropoffDatetime)

	rpt := trps[0].Timestamp
	trpt := ptime.Add(to).Format(outputDateTimeLayout)
	if rpt != trpt {
		t.Errorf("Ride start time adjustment wrong! Expected: %v, Actual: %v", trpt, rpt)
	}
	rdt := trps[len(trps)-1].Timestamp
	trdt := dtime.Add(to).Format(outputDateTimeLayout)
	if rdt != trdt {
		t.Errorf("Ride end time adjustment wrong! Expected: %v, Actual: %v", trdt, rdt)
	}

	// get ridepoints with speedup factor 10
	trps, err = tr.ridePoints(now, to, 10)
	if err != nil {
		t.Error(err)
	}

	ptime = time.Time(tr.TPepPickupDatetime)

	// diff of pickup time to dataset start time and speedup by factor 10
	d1 := ptime.Sub(dsst) / 10

	dtime = time.Time(tr.TPepDropoffDatetime)

	// diff of drop-off time to dataset start time and speedup by factor 10
	d2 := dtime.Sub(dsst) / 10

	rpt = trps[0].Timestamp
	trpt = ptime.Add(to).Add(d1 * -9).Format(outputDateTimeLayout)
	if rpt != trpt {
		t.Errorf("Ride start time adjustment wrong! Expected: %v, Actual: %v", trpt, rpt)
	}
	rdt = trps[len(trps)-1].Timestamp
	trdt = dtime.Add(to).Add(d2 * -9).Format(outputDateTimeLayout)
	if rdt != trdt {
		t.Errorf("Ride end time adjustment wrong! Expected: %v, Actual: %v", trdt, rdt)
	}
}

func TestIsValidTest(t *testing.T) {
	tr1 := &taxiRide{}
	if tr1.valid() {
		t.Errorf("Ride shouldn't be valid %v", tr1)
	}

	pt, _ := parseInputTimeString("2015-01-04 20:00:40")

	tr2 := &taxiRide{
		TPepPickupDatetime: rideTime(pt),
	}
	if tr2.valid() {
		t.Errorf("Ride shouldn't be valid %v", tr2)
	}

	pt1, _ := parseInputTimeString("2015-01-04 20:00:40")
	pt2, _ := parseInputTimeString("2015-01-05 02:00:41")
	tr3 := &taxiRide{
		TPepPickupDatetime:  rideTime(pt1),
		TPepDropoffDatetime: rideTime(pt2),
	}
	if tr3.valid() {
		t.Errorf("Ride shouldn't be valid %v", tr3)
	}

	pt3, _ := parseInputTimeString("2015-01-05 02:00:40")
	tr4 := &taxiRide{
		TPepPickupDatetime:  rideTime(pt1),
		TPepDropoffDatetime: rideTime(pt3),
	}
	if !tr4.valid() {
		t.Errorf("Ride should be valid %v", tr4)
	}

}

func TestUUID(t *testing.T) {
	u1, err := newUUID()
	if err != nil {
		t.Error(err)
	}

	u2, err := newUUID()
	if err != nil {
		t.Error(err)
	}

	if u1 == u2 {
		t.Error("newUUID generated UUIDs are NOT unique!")
	}
}
