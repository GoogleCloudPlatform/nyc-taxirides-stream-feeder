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
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

// PubSubClient is an authenticated Cloud Pub/Sub client
type PubSubClient struct {
	*pubsub.Client
	topic    *pubsub.Topic
	debugLog debugging
}

// NewPubSubService creates new authenticated Cloud Pub/Sub client with the given
// service account. If no service account is provided, the default
// auth context is used.
func NewPubSubService(svcAccJSON string, projectID string, topic string, debugLog debugging) (*PubSubClient, error) {
	debugLog.Println("Connecting to Google Cloud Pub/Sub ...")
	var opts []option.ClientOption
	if _, err := os.Stat(svcAccJSON); err == nil {
		opts = append(opts, option.WithServiceAccountFile(svcAccJSON))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}

	debugLog.Printf("Checking if Pub/Sub topic '%s' exists ...\n", topic)
	t := c.Topic(topic)
	ok, err := t.Exists(ctx)
	if err != nil {
		err2 := c.Close()
		if err2 != nil {
			log.Println(err2)
		}
		return nil, err
	}

	if !ok {
		err = c.Close()
		if err != nil {
			log.Println(err)
		}
		return nil, fmt.Errorf("Topic %v doesn't exist", topic)
	}

	debugLog.Println("Connection to Google Cloud Pub/Sub successful.")
	return &PubSubClient{c, t, debugLog}, nil
}
