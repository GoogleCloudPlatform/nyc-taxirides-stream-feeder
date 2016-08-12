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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/namsral/flag"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"
)

const (
	gcsScope              = storage.DevstorageReadOnlyScope
	datasetDateTimeLayout = "2006-01-02 15:04:05" // format of dataset TPepPickupDatetime
	outputDateTimeLayout  = "2006-01-02T15:04:05.99999Z07:00"

	serviceAccountJSONFile = "service-account.json"
	publishFlushCycle      = 100 * time.Millisecond
)

var (
	projectID             = flag.String("project", "", "Your cloud project ID.")
	bucketName            = flag.String("bucket", "", "The name of the bucket within your project.")
	filePrefix            = flag.String("filePrefix", "", "FilePrefix path for data to load.")
	dsLocation            = flag.String("datasetLocation", "America/New_York", "Dataset time location. See IANA Time Zone database for valid values.")
	dsBaseTime            = flag.String("datasetBaseTime", "2015-01-04 20:00:00", "Dataset first ride time.")
	topicName             = flag.String("pubsubTopic", "", "Name to PubSub topic to publish records to")
	fileType              = flag.String("t", "json", "FileType 'csv' or 'json'. Default 'json'.")
	flLoop                = flag.Bool("loop", false, "Loop through input files forever. (default false)")
	flDebug               = flag.Bool("debug", false, "Enable debug output to stdout. (default false)")
	speedupFactor         = flag.Int("speedup", 1, "Factor to speedup push to pubsub. 60 pushes 1h of data in 1 minute.")
	skipRidesMod          = flag.Int("skipRides", 1, "Only send every mod n'th ride to pubsub to lower qps.")
	skipOffset            = flag.Int("skipOffset", 0, "skipride offset to enable scaleout, if > 0 startRefTime needs to be set.")
	refTime               = flag.String("startRefTime", "", "Set ref time for scalout. If refTime is set, loop is currently not possible Format: 2015-01-04 20:00:00")
	maxParallelSchedulers = flag.Int("maxSchedulers", 5000, "Max parallel schedulers to restrain memory usage")

	timeLocation *time.Location

	version string // set by linker -X
	build   string // set by linker -X
	// maxPickupTime time.Time
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("version:    ", version)
		fmt.Println("build:      ", build)
		os.Exit(0)
	}

	flag.String(flag.DefaultConfigFlagname, "", "path to config file")
	flag.Parse()

	debugLog := debugging(*flDebug)
	if debugLog {
		fmt.Println("Debugging enabled - ")
		fmt.Printf("Running feeder version %v, build: %v\n", version, build)
	}

	if *bucketName == "" {
		log.Fatalf("Bucket argument is required. See --help.")
	}

	if *projectID == "" {
		log.Fatalf("Project argument is required. See --help.")
	}

	if *filePrefix == "" {
		log.Fatalf("FilePrefix argument is required. See --help.")
	}

	if *fileType != "csv" && *fileType != "json" {
		log.Fatalf("Only 'json' or 'csv' supported for FileType")
	}

	if *speedupFactor <= 0 {
		log.Fatalf("Invalid speedup factor of value %v", *speedupFactor)
	}

	if *skipOffset > 0 && *refTime == "" {
		log.Fatalf("If skipOffset is larger than 0 you need to set startRefTime! See --help.")
	}

	if *skipRidesMod > 1 && *skipOffset >= *skipOffset {
		log.Fatalf("skipOffset can only be 0 to %v (skipRides value - 1)", *skipRidesMod-1)
	}

	var err error
	timeLocation, err = time.LoadLocation(*dsLocation)
	if err != nil {
		log.Fatalf("Dataset time location lookup failed: %v", err)
	}

	// *** BEGIN - Initialize Cloud Storage Client ***
	// Authentication is provided by the gcloud tool when running locally, and
	// by the associated service account when running on Compute Engine.
	gcs, err := NewGCSService(serviceAccountJSONFile, debugLog, gcsScope)
	if err != nil {
		log.Fatalf("Couldn't connect to Google Cloud Storage: %v", err)
	}
	defer gcs.Close()
	// *** END - Initialize Cloud Storage Client ***

	// *** BEGIN - Initialize PubSub Client ***
	pb, err := NewPubSubService(serviceAccountJSONFile, *projectID, *topicName, debugLog)
	if err != nil {
		log.Fatalf("Couldn't connect to Google Cloud Pub/Sub: %v", err)
	}
	defer pb.Close()

	// setup buffered publishing to pubsub
	psmch := make(chan *pubsub.Message, 5e5)
	bpsp := &bufferedPubsubPublisher{
		client:   pb,
		debugLog: debugLog,
	}
	bpsp.run(psmch)
	// *** END - Initialize PubSub Client ***

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Cleaning up resources before exiting!")
		bpsp.stop()
		gcs.Close()
		pb.Close()
		os.Exit(1)
	}()

	debugLog.Printf("Reading files from bucket %v:\n", *bucketName+"/"+*filePrefix)
	inputFiles, err := gcs.list(*bucketName, *filePrefix)
	if err != nil {
		log.Fatalf("Unable to get list of files for bucket: %v/%v: %v", *bucketName, *filePrefix, err)
	}

	// start periodic debug statistic printout
	debugLog.printDebugStats()

	// WaitGroup to push ride points to pubsub
	var wg sync.WaitGroup
	rpsch := make(chan *ridePointScheduler, *maxParallelSchedulers)
	for i := 0; i < *maxParallelSchedulers; i++ {
		go ridePointSchedulerWorker(&wg, rpsch, psmch)
	}
	processFiles(&inputFiles, gcs, rpsch, debugLog)

	debugLog.Printf("Waiting for rides queue to drain... ")
	// wait till all points are scheduled for publishing
	wg.Wait()
	close(psmch)
	// wait for the publisher buffer to be drained?
	<-bpsp.done()
	log.Printf("Good bye! - Done pushing ride points to pubsub")
}

func pubsubClient(ctx context.Context) (*pubsub.Client, error) {
	if _, err := os.Stat(serviceAccountJSONFile); err != nil {
		return pubsub.NewClient(ctx, *projectID)
	}
	serviceAccountJSON, err := ioutil.ReadFile(serviceAccountJSONFile)
	if err != nil {
		return nil, err
	}

	conf, err := google.JWTConfigFromJSON(serviceAccountJSON, pubsub.ScopePubSub, pubsub.ScopeCloudPlatform)
	if err != nil {
		return nil, err
	}
	return pubsub.NewClient(ctx, *projectID, option.WithTokenSource(conf.TokenSource(ctx)))
}

// listFiles for passed bucketName filtered by passed FilePrefix
func listFiles(svc *storage.Service, bucketName string, filePrefix string) ([]string, error) {
	// List all objects in a bucket using pagination
	var files []string
	token := ""
	for {
		call := svc.Objects.List(bucketName)
		call.Prefix(filePrefix)
		if token != "" {
			call = call.PageToken(token)
		}
		res, err := call.Do()
		if err != nil {
			return nil, err
		}
		for _, object := range res.Items {
			files = append(files, object.Name)
		}
		if token = res.NextPageToken; token == "" {
			break
		}
	}
	return files, nil
}

func ridePointSchedulerWorker(wg *sync.WaitGroup, rpsch <-chan *ridePointScheduler, ch chan *pubsub.Message) {
	for rps := range rpsch {
		wg.Add(1)
		rps.run(ch)
		wg.Done()
	}
}

func processFiles(files *[]string, gcs *GCSclient, rpsch chan<- *ridePointScheduler, debugLog debugging) {

	// set dataset start time to time.Now().In(timeLocation) and calculate
	// offset to original dataset start time
	dsStartTime, err := parseInputTimeString(*dsBaseTime)
	if err != nil {
		log.Fatalf("Unable to parse dataset base time %v: %v", *dsBaseTime, err)
	}

	var newRefDSStartTime time.Time
	if *refTime == "" {
		newRefDSStartTime = time.Now()
	} else {
		newRefDSStartTime, err = parseInputTimeString(*refTime)
		if err != nil {
			log.Fatalf("Unable to parse refTime config %v", err)
		}
	}

	// defered load of inputFiles
	for {
		var maxPickupTime time.Time
		timeOffset := newRefDSStartTime.Sub(dsStartTime)
		debugLog.Printf("Using new dataset base time: %v", newRefDSStartTime)
		for _, file := range *files {

			f, err := gcs.read(*bucketName, file)
			if err != nil {
				log.Fatalf("Unable to get file from bucket: "+*bucketName+"/"+file, err)
			}
			switch *fileType {
			case "json":
				// file will be closed in ridesFromJSONRawIOReader
				taxiRides, err := ridesFromJSONRawBytes(f)
				if err != nil {
					log.Fatalf("Error in reading %v from GCS: %v", file, err)
				}

				debugLog.Printf("Reading %v rides from %v", len(taxiRides.Rides), file)
				for j, r := range taxiRides.Rides {
					if !r.valid() || (j+1+*skipOffset)%*skipRidesMod != 0 {
						continue
					}

					// schedule the taxi ride points to be published to pubsub
					rps := &ridePointScheduler{
						taxiRide:      r,
						refTime:       newRefDSStartTime,
						timeOffset:    timeOffset,
						speedupFactor: *speedupFactor,
						debugLog:      debugLog,
					}

					// start pushing ride points
					rpsch <- rps
				}

				// Rides in input files are in chronological order. To save memory usage we load them deferred
				// take the last ride in input file and use pickupTime to calculate sleep time till load of next
				// input file from list
				pt := time.Time(taxiRides.Rides[len(taxiRides.Rides)-1].TPepPickupDatetime)
				if maxPickupTime.IsZero() || maxPickupTime.Before(pt) {
					maxPickupTime = pt
				}
				// calculate time till next file to load with speedup considered
				rtd := time.Now().Sub(newRefDSStartTime)
				d := int64(pt.Add(timeOffset).Sub(newRefDSStartTime).Nanoseconds()/int64(*speedupFactor)) - rtd.Nanoseconds()
				time.Sleep(time.Duration(d) - (10 * time.Second))
			default:
				log.Fatalf("Unsupported file input format")
			}
		}
		if !*flLoop {
			break
		}
		newRefDSStartTime = newRefDSStartTime.Add(time.Duration(int64(maxPickupTime.Add(timeOffset).Sub(newRefDSStartTime).Nanoseconds() / int64(*speedupFactor))))
		if newRefDSStartTime.Before(time.Now()) {
			newRefDSStartTime = time.Now()
		}
	}
}

func parseInputTimeString(timestamp string) (time.Time, error) {
	return time.ParseInLocation(datasetDateTimeLayout, timestamp, timeLocation)
}

func parseOutputTimeString(timestamp string) (time.Time, error) {
	return time.ParseInLocation(outputDateTimeLayout, timestamp, timeLocation)
}
