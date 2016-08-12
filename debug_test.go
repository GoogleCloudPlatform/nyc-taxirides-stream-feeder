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
	"log"
	"os"
	"testing"
	"time"
)

var schedDebugCounterKeys = []string{
	ridesLoadedKey,
	ridesProcessedKey,
	ridesInvalidKey,
	pointsLoadedKey,
	pointsScheduledKey,
	pointsFailedKey,
}

var pubDebugCounterKeys = []string{
	sentMsgsKey,
	failedMsgsKey,
	publishBacklogKey,
}

func setup() {
	var err error
	timeLocation, err = time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalf("Dataset time location lookup failed: %v", err)
	}

	for i, dck := range schedDebugCounterKeys {
		schedDebugCounter.Add(dck, 100)
		schedDebugCounter.Add(dck, int64(-i))
	}
	for i, dck := range pubDebugCounterKeys {
		pubDebugCounter.Add(dck, 100)
		pubDebugCounter.Add(dck, int64(-i))
	}
}

func TestMain(m *testing.M) {
	setup()
	retCode := m.Run()
	os.Exit(retCode)
}

func TestRidesLoaded(t *testing.T) {
	if ridesLoaded() != 100 {
		t.Errorf("Inconsistency in Debug Counter for Rides loaded. Expected: %v, actual: %v", 100, ridesLoaded())
	}
}

func TestRidesProcessed(t *testing.T) {
	if ridesProcessed() != 99 {
		t.Errorf("Inconsistency in Debug Counter for Rides processed. Expected: %v, actual: %v", 100, ridesProcessed())
	}
}

func TestRidesInvalid(t *testing.T) {
	if ridesInvalid() != 98 {
		t.Errorf("Inconsistency in Debug Counter for Rides invalid. Expected: %v, actual: %v", 100, ridesInvalid())
	}
}

func TestPointsLoaded(t *testing.T) {
	if pointsLoaded() != 97 {
		t.Errorf("Inconsistency in Debug Counter for Points loaded. Expected: %v, actual: %v", 100, pointsLoaded())
	}
}

func TestPointsScheduled(t *testing.T) {
	if pointsScheduled() != 96 {
		t.Errorf("Inconsistency in Debug Counter for Points scheduled. Expected: %v, actual: %v", 100, pointsScheduled())
	}
}

func TestPointsFailed(t *testing.T) {
	if pointsFailed() != 95 {
		t.Errorf("Inconsistency in Debug Counter for Points failed. Expected: %v, actual: %v", 100, pointsFailed())
	}
}

func TestMessagesSent(t *testing.T) {
	if messagesSent() != 100 {
		t.Errorf("Inconsistency in Debug Counter for Messages sent. Expected: %v, actual: %v", 100, messagesSent())
	}
}

func TestMessagesFailed(t *testing.T) {
	if messagesFailed() != 99 {
		t.Errorf("Inconsistency in Debug Counter for Messages failed. Expected: %v, actual: %v", 100, messagesFailed())
	}
}

func TestPublishBacklog(t *testing.T) {
	if publishBacklog() != 98 {
		t.Errorf("Inconsistency in Debug Counter for Publisher backlog. Expected: %v, actual: %v", 100, publishBacklog())
	}
}
