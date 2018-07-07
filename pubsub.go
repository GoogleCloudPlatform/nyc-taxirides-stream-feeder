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

	"cloud.google.com/go/pubsub"
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
func NewPubSubService(conf *config, debugLog debugging) (*PubSubClient, error) {
	debugLog.Println("Connecting to Google Cloud Pub/Sub ...")
	ctx := context.Background()

	c, err := pubsub.NewClient(ctx, conf.ProjectID)
	if err != nil {
		return nil, err
	}

	debugLog.Printf("Checking if Pub/Sub topic '%s' exists ...\n", conf.Topic)
	t := c.Topic(conf.Topic)
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
		return nil, fmt.Errorf("Topic %v doesn't exist", conf.Topic)
	}

	debugLog.Println("Connection to Google Cloud Pub/Sub successful.")
	return &PubSubClient{c, t, debugLog}, nil
}
