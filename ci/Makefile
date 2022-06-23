HOSTNAME        := gcr.io
PROJECT         := stargateio
NAME            := stargate-builder
VERSION         := v1.0.8
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

push: check-image push-exe

check-image:
	$(eval image := $(shell gcloud container images list-tags gcr.io/stargateio/stargate-builder | grep "${VERSION}"))
	@if [ -n "${image}" ]; then \
		echo -n "The image with the version ${VERSION} exists, do you want to overwrite [Y/n]?" \
			&& read ans \
			&& [ $${ans:-y} = y ] \
			|| (echo "Aborted!"; exit 1) \
	fi

push-exe:
	@echo Push docker image ${IMG_GCP} to GCP..
	docker push ${IMG_GCP}
