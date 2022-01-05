HOSTNAME        := gcr.io
PROJECT         := stargateio
NAME            := stargate-builder
VERSION         := v1.0.6
TAG             := ${VERSION}
IMG             := ${NAME}:${TAG}
IMG_GCP         := ${HOSTNAME}/${PROJECT}/${IMG}

.PHONY : info build

info:
	@echo Version ${VERSION}
	@echo GCP image ${IMG_GCP}

build: build-docker

build-docker:
	@echo Building docker image and tagging..
	docker build -t ${IMG} .
	docker tag ${IMG} ${IMG_GCP}

push:
	@echo Push docker image ${IMG_GCP} to GCP..
	docker push ${IMG_GCP}
