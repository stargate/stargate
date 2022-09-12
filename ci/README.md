# Stargate Continuous Integration

This folder contains necessary files for the docker image being used during the CI:

* `Dockerfile` - docker file for the container that will be used during the CI
* `test.sh` - script that is actually executed during the CI
* `Makefile` - small Makefile that enables easier building and pushing of a new CI docker image

*Note that we run the CI on the GCP using [Cloud Build](https://cloud.google.com/build).
The docker image is directly referenced in the [cloudbuild.yaml](../cloudbuild.yaml), [cloudbuild-3_11.yaml](../cloudbuild-3_11.yaml), , [cloudbuild-4_0.yaml](../cloudbuild-4_0.yaml) and [cloudbuild-dse.yaml](../cloudbuild-dse.yaml) files which represent the jobs we do during the CI.*

## Updating the docker image

### Prerequisites

In order to push the new docker image to the GCP image registry `gcr.io/stargateio`, besides having permissions to do so, you should authenticate and set up your docker configuration using:

```bash
gcloud auth login
gcloud auth configure-docker
```

That said, it's assumed that `gcloud` is available on the system, as it's going to be used for getting the existing image tags in the [Makefile](./Makefile).

NOTE: if you see warning like this for "gcloud auth configure-docker":

```
WARNING: `docker-credential-gcloud` not in system PATH.
gcloud's Docker credential helper can be configured but it will not work until this is corrected.
```

you will need to resolve this by making sure said command from `gcloud` package:

    ./google-cloud-sdk/bin/docker-credential-gcloud

is in the $PATH.

### Process

* Perform wanted updates related to the image.
* Bump the `VERSION` variable in the [Makefile](Makefile).
* Run `make info` and confirm that the image you are about to build and push has a correct tag.
* Run `make build push`.
It will build the image and push to the GCP repository.
* Update all the `cloudbuild*.yaml` files with the new version of the image.
* Commit and create a pull request.
Note that the created pull request will run the CI using the new image.
