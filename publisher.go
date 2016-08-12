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

	"cloud.google.com/go/pubsub"
)

// bufferedPubsubPublisher enables message publish pooling
// migrated to use pubsub client batching
type bufferedPubsubPublisher struct {
	client   *PubSubClient
	debugLog debugging
	donec    chan struct{}
}

func (p *bufferedPubsubPublisher) run(ch <-chan *pubsub.Message) {
	p.debugLog.Printf("bufferedPubsubPublisher started...")
	p.donec = make(chan struct{})
	prs := make(chan *pubsub.PublishResult, 5e5)
	ctx := context.Background()
	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					p.stop()
					break
				}
				if p.debugLog {
					pubDebugCounter.Add(publishBacklogKey, 1)
				}
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
			if id, err := r.Get(ctx); err != nil {
				log.Printf("Error publishing message %s to pubsub: %v", id, err)
				if p.debugLog {
					pubDebugCounter.Add(failedMsgsKey, 1)
				}
			}
			if p.debugLog {
				pubDebugCounter.Add(publishBacklogKey, -1)
				pubDebugCounter.Add(sentMsgsKey, 1)
			}
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
