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
	"context"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

// bufferedPubsubPublisher enables message publish pooling
// migrated to use pubsub client batching
type bufferedPubsubPublisher struct {
	client   *PubSubClient
	debugLog debugging
	donec    chan struct{}
	metrics  *metrics
}

func (p *bufferedPubsubPublisher) run(ch <-chan *pubsub.Message, maxmsgs int) {
	p.debugLog.Printf("bufferedPubsubPublisher started...")
	p.donec = make(chan struct{})
	prs := make(chan *pubsub.PublishResult, maxmsgs)
	ctx := context.Background()
	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					p.stop()
					break
				}
				p.metrics.messagesBacklogInc()
				// send response to response channel for async handling
				prs <- p.client.topic.Publish(ctx, msg)

			case <-p.done():
				p.debugLog.Println("Exiting publisher...")
				p.client.topic.Stop()
				return
			}
		}
	}()

	// pubsub response handling
	go func() {
		for r := range prs {
			ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
			if id, err := r.Get(ctx2); err != nil {
				log.Printf("Error publishing message %s to pubsub: %v", id, err)
				p.metrics.messagesFailedInc()
			}
			cancel()
			p.metrics.messagesBacklogDec()
			p.metrics.messagesSentInc()
		}
		p.debugLog.Println("Processing publish results channel closed")
		p.stop()
	}()
}

func (p *bufferedPubsubPublisher) done() <-chan struct{} {
	return p.donec
}

func (p *bufferedPubsubPublisher) stop() {
	close(p.donec)
}
