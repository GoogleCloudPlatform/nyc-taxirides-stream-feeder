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
	"log"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/namsral/flag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	validator "gopkg.in/go-playground/validator.v9"

	"cloud.google.com/go/pubsub"
	storage "google.golang.org/api/storage/v1"
)

const (
	gcsScope              = storage.DevstorageReadOnlyScope
	datasetDateTimeLayout = "2006-01-02 15:04:05" // format of dataset TPepPickupDatetime
	outputDateTimeLayout  = "2006-01-02T15:04:05.99999Z07:00"
	publishFlushCycle     = 100 * time.Millisecond
)

var (
	version string // set by linker -X
	build   string // set by linker -X

	timeLoc  *time.Location
	debugLog debugging
)

type config struct {
	Debug               bool
	ProjectID           string `validate:"required"`
	Bucket              string `validate:"required"`
	FilePrefix          string `validate:"required"`
	DSLocation          string
	DSBaseTime          string
	Topic               string
	FileType            string `validate:"is-csv-or-json"`
	Loop                bool
	Speedup             int `validate:"min=0"`
	SkipRides           int
	SkipOffset          int
	MaxBufferedMessages int
	RefTime             string
	Schedulers          int
	Port                int
}

func processFlags(c *config) error {
	flag.String(flag.DefaultConfigFlagname, "", "path to config file")
	flag.StringVar(&c.ProjectID, "project", "", "Your cloud project ID.")
	flag.StringVar(&c.Bucket, "bucket", "", "The name of the bucket within your project.")
	flag.StringVar(&c.FilePrefix, "filePrefix", "", "FilePrefix path for data to load.")
	flag.StringVar(&c.DSLocation, "datasetLocation", "America/New_York", "Dataset time location. See IANA Time Zone database for valid values.")
	flag.StringVar(&c.DSBaseTime, "datasetBaseTime", "2015-01-04 20:00:00", "Dataset first ride time.")
	flag.StringVar(&c.Topic, "pubsubTopic", "", "Name to PubSub topic to publish records to")
	flag.StringVar(&c.FileType, "t", "json", "FileType 'csv' or 'json'. Default 'json'.")
	flag.BoolVar(&c.Loop, "loop", false, "Loop through input files forever. (default false)")
	flag.BoolVar(&c.Debug, "debug", false, "Enable debug output to stdout. (default false)")
	flag.IntVar(&c.Speedup, "speedup", 1, "Factor to speedup push to pubsub. 60 pushes 1h of data in 1 minute.")
	flag.IntVar(&c.SkipRides, "skipRides", 1, "Only send every mod n'th ride to pubsub to lower qps.")
	flag.IntVar(&c.SkipOffset, "skipOffset", 0, "skipride offset to enable scaleout, if > 0 startRefTime needs to be set.")
	flag.IntVar(&c.MaxBufferedMessages, "maxBufferedMsgs", 5e5, "Max buffered messages if process gets back pressure from Pub/Sub")
	flag.StringVar(&c.RefTime, "startRefTime", "", "Set ref time for scalout. If refTime is set, loop is currently not possible Format: 2015-01-04 20:00:00")
	flag.IntVar(&c.Schedulers, "maxSchedulers", 5000, "Max parallel schedulers to restrain memory usage")
	flag.IntVar(&c.Port, "port", 8080, "Port for Prometheus metrics HTTP endpoint")
	flag.Parse()

	var err error
	timeLoc, err = time.LoadLocation(c.DSLocation)
	if err != nil {
		log.Fatalf("Dataset time location lookup failed: %v", err)
	}

	debugLog = debugging(c.Debug)

	debugLog.Printf("- Debugging enabled - \n")
	debugLog.Println("Debugging enabled - ")
	debugLog.Printf("Running feeder version %v, build: %v\n", version, build)
	debugLog.Printf("-- Configuration --\n")
	s := reflect.ValueOf(c).Elem()
	for i := 0; i < s.NumField(); i++ {
		n := s.Type().Field(i).Name
		f := s.Field(i)
		debugLog.Printf("%v: %v\n", n, f.Interface())
	}
	v := validator.New()
	v.RegisterValidation("is-csv-or-json", ValidateCSVorJSON)
	return v.Struct(c)
}

func ValidateCSVorJSON(fl validator.FieldLevel) bool {
	return strings.ToLower(fl.Field().String()) == "csv" ||
		strings.ToLower(fl.Field().String()) == "json"
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("version:    ", version)
		fmt.Println("build:      ", build)
		os.Exit(0)
	}

	config := &config{}
	err := processFlags(config)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	if config.SkipOffset > 0 && config.RefTime == "" {
		log.Fatalf("If skipOffset is larger than 0 you need to set startRefTime! See --help.")
	}

	if config.SkipRides > 1 && config.SkipRides > config.SkipRides-1 {
		log.Fatalf("skipOffset can only be 0 to %v (skipRides value - 1)", config.SkipRides-1)
	}

	// *** BEGIN - Initialize Cloud Storage Client ***
	// Authentication is provided by the gcloud tool when running locally, and
	// by the associated service account when running on Compute Engine.
	gcs, err := NewGCSService(debugLog, gcsScope)
	if err != nil {
		log.Fatalf("Couldn't connect to Google Cloud Storage: %v", err)
	}
	defer gcs.Close()
	// *** END - Initialize Cloud Storage Client ***

	// *** BEGIN - Initialize PubSub Client ***
	pb, err := NewPubSubService(config, debugLog)
	if err != nil {
		log.Fatalf("Couldn't connect to Google Cloud Pub/Sub: %v", err)
	}
	defer pb.Close()

	// new metrics instance
	fmetrics := NewFeederMetrics()

	// setup buffered publishing to pubsub
	psmch := make(chan *pubsub.Message, config.MaxBufferedMessages)
	bpsp := &bufferedPubsubPublisher{
		client:   pb,
		metrics:  fmetrics,
		debugLog: debugLog,
	}
	bpsp.run(psmch, config.MaxBufferedMessages)
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

	debugLog.Printf("Reading files from bucket %v:\n", config.Bucket+"/"+config.FilePrefix)
	inputFiles, err := gcs.list(config.Bucket, config.FilePrefix)
	if err != nil {
		log.Fatalf("Unable to get list of files for bucket: %v/%v: %v", config.Bucket, config.FilePrefix, err)
	}

	debugLog.Println("Setting up Prometheus metrics endpoint...")
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", config.Port), nil))
	}()

	// start periodic debug statistic printout
	// debugLog.printDebugStats()

	// prometheus

	// WaitGroup to push ride points to pubsub
	var wg sync.WaitGroup
	rpsch := make(chan *ridePointScheduler, config.Schedulers)
	for i := 0; i < config.Schedulers; i++ {
		go ridePointSchedulerWorker(&wg, rpsch, psmch, config)
	}
	processFiles(config, &inputFiles, gcs, rpsch, fmetrics, debugLog)

	debugLog.Printf("Waiting for rides queue to drain... ")
	// wait till all points are scheduled for publishing
	wg.Wait()
	close(psmch)
	// wait for the publisher buffer to be drained?
	<-bpsp.done()
	log.Printf("Good bye! - Done pushing ride points to pubsub")
}

func ridePointSchedulerWorker(wg *sync.WaitGroup, rpsch <-chan *ridePointScheduler,
	ch chan *pubsub.Message, c *config) {
	for rps := range rpsch {
		wg.Add(1)
		rps.run(ch)
		wg.Done()
	}
}

func processFiles(c *config, files *[]string, gcs *GCSclient, rpsch chan<- *ridePointScheduler,
	fmetrics *metrics, debugLog debugging) {

	// set dataset start time to time.Now().In(timeLocation) and calculate
	// offset to original dataset start time
	dsStartTime, err := parseInputTimeString(c.DSBaseTime)
	if err != nil {
		log.Fatalf("Unable to parse dataset base time %v: %v", c.DSBaseTime, err)
	}

	var newRefDSStartTime time.Time
	if c.RefTime == "" {
		newRefDSStartTime = time.Now()
	} else {
		newRefDSStartTime, err = parseInputTimeString(c.RefTime)
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

			f, err := gcs.read(c.Bucket, file)
			if err != nil {
				log.Fatalf("Unable to get file from bucket: "+c.Bucket+"/"+file, err)
			}
			switch c.FileType {
			case "json":
				// file will be closed in ridesFromJSONRawIOReader
				taxiRides, err := ridesFromJSONRawBytes(f)
				if err != nil {
					log.Fatalf("Error in reading %v from GCS: %v", file, err)
				}

				debugLog.Printf("Reading %v rides from %v", len(taxiRides.Rides), file)
				for j, r := range taxiRides.Rides {
					if !r.valid(fmetrics) || (j+1+c.SkipOffset)%c.SkipRides != 0 {
						continue
					}

					// schedule the taxi ride points to be published to pubsub
					rps := &ridePointScheduler{
						taxiRide:      r,
						refTime:       newRefDSStartTime,
						timeOffset:    timeOffset,
						speedupFactor: c.Speedup,
						metrics:       fmetrics,
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
				d := int64(pt.Add(timeOffset).Sub(newRefDSStartTime).Nanoseconds()/int64(c.Speedup)) - rtd.Nanoseconds()
				time.Sleep(time.Duration(d) - (10 * time.Second))
			default:
				log.Fatalf("Unsupported file input format")
			}
		}
		if !c.Loop {
			break
		}
		newRefDSStartTime = newRefDSStartTime.Add(time.Duration(int64(maxPickupTime.Add(timeOffset).Sub(newRefDSStartTime).Nanoseconds() / int64(c.Speedup))))
		if newRefDSStartTime.Before(time.Now()) {
			newRefDSStartTime = time.Now()
		}
	}
}

func parseInputTimeString(timestamp string) (time.Time, error) {
	return time.ParseInLocation(datasetDateTimeLayout, timestamp, timeLoc)
}

func parseOutputTimeString(timestamp string) (time.Time, error) {
	return time.ParseInLocation(outputDateTimeLayout, timestamp, timeLoc)
}
