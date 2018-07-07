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
	"github.com/prometheus/client_golang/prometheus"
)

const (
	ridesLoadedKey     = "loaded"
	ridesProcessedKey  = "processed"
	ridesInvalidKey    = "invalid"
	pointsLoadedKey    = "loaded"
	pointsScheduledKey = "scheduled"
	pointsFailedKey    = "failed"
	sentMsgsKey        = "sent"
	failedMsgsKey      = "failed"
	publishBacklogKey  = "backlog"
)

type metrics struct {
	rides    *prometheus.CounterVec
	points   *prometheus.CounterVec
	messages *prometheus.CounterVec
	backlog  prometheus.Gauge
}

func NewFeederMetrics() *metrics {
	rides := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ride_counter",
			Help: "Ride counter - loaded, processed, invalid",
		},
		[]string{"type"},
	)
	prometheus.MustRegister(rides)

	points := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "point_counter",
			Help: "Point counter - loaded, scheduled, failed",
		},
		[]string{"type"},
	)
	prometheus.MustRegister(points)

	messages := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_counter",
			Help: "Message counter - sent, failed, backlog",
		},
		[]string{"type"},
	)
	prometheus.MustRegister(messages)

	backlog := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pubsub_backlog",
			Help: "Pub/Sub message backlog",
		},
	)
	prometheus.MustRegister(backlog)

	return &metrics{
		rides:    rides,
		points:   points,
		messages: messages,
		backlog:  backlog,
	}
}

func (m *metrics) ridesLoadedInc() {
	m.rides.WithLabelValues("loaded").Inc()
}

func (m *metrics) ridesProcessedInc() {
	m.rides.WithLabelValues("processed").Inc()
}

func (m *metrics) ridesInvalidInc() {
	m.rides.WithLabelValues("invalid").Inc()
}

func (m *metrics) pointsLoadedAdd(c int) {
	m.points.WithLabelValues("loaded").Add(float64(c))
}

func (m *metrics) pointsScheduledInc() {
	m.points.WithLabelValues("scheduled").Inc()
}

func (m *metrics) pointsFailedAdd(c int) {
	m.points.WithLabelValues("failed").Add(float64(c))
}

func (m *metrics) messagesSentInc() {
	m.messages.WithLabelValues("sent").Inc()
}

func (m *metrics) messagesFailedInc() {
	m.messages.WithLabelValues("failed").Inc()
}

func (m *metrics) messagesBacklogInc() {
	m.backlog.Inc()
}

func (m *metrics) messagesBacklogDec() {
	m.backlog.Dec()
}
