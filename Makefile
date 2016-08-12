# Copyright 2018 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


PACKAGE           := feeder
GOOS              := linux

DATE              := $(shell TZ=UTC date +%FT%T)Z
VERSION           := $(shell git describe --always)


PROJECT           := $(shell gcloud config list --format 'value(core.project)')
GCR               ?= gcr.io

DOCKER_FILE       := Dockerfile.$(GOOS)
SRCS              := $(shell find . -type f -name '*.go' -not -path "./vendor/*")

LDFLAGS           := '-X main.version=$(VERSION) -X main.build=$(DATE)'

$(PACKAGE): $(SRCS)
	go build -o $@ -ldflags $(LDFLAGS) $(SRC)

.PHONY: container push-gcr
all: container push-gcr

container: $(SRCS) $(DOCKER_FILE)
	# builds the binary and packages it into an alpine container
	docker build --build-arg ldflags=$(LDFLAGS) --build-arg package=$(PACKAGE) \
	--build-arg goos=$(GOOS) -t $(PACKAGE):$(VERSION) -f $(DOCKER_FILE) .


push-gcr: container
	# push to google cloud container registry
	docker tag $(PACKAGE):$(VERSION) $(GCR)/$(PROJECT)/$(PACKAGE):$(VERSION)
	docker tag $(PACKAGE):$(VERSION) $(GCR)/$(PROJECT)/$(PACKAGE):latest
	docker push $(GCR)/$(PROJECT)/$(PACKAGE):$(VERSION)
	docker push $(GCR)/$(PROJECT)/$(PACKAGE):latest