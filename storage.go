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
	"io/ioutil"
	"sort"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSclient is an authenticated Cloud Storage client
type GCSclient struct {
	*storage.Client
	debugLog debugging
}

// NewGCSService creates new authenticated Cloud Storage client with the given
// scope and service account. If no service account is provided, the default
// auth context is used.
func NewGCSService(debugLog debugging, scopes ...string) (*GCSclient, error) {
	debugLog.Println("Connecting to Google Cloud Storage ...")
	var opts []option.ClientOption
	opts = append(opts, option.WithScopes(scopes...))
	c, err := storage.NewClient(context.Background(), opts...)

	if err != nil {
		return nil, err
	}
	return &GCSclient{c, debugLog}, nil
}

// list for passed bucketName filtered by passed FilePrefix
func (svc *GCSclient) list(bucketName string, filePrefix string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	// List all objects in a bucket using pagination
	var files []string
	it := svc.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: filePrefix})
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		files = append(files, obj.Name)
	}
	sort.Strings(files)
	return files, nil
}

func (svc *GCSclient) read(bucketName, filepath string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	fr, err := svc.Bucket(bucketName).Object(filepath).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	c, err := ioutil.ReadAll(fr)
	if err != nil {
		return nil, err
	}
	return c, nil
}
