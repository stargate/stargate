# Stargate Continuous Integration

This folder contains necessary files for the docker image being used during the CI:

* `Dockerfile` - docker file for the container that will be used during the CI
* `test.sh` - script that is actually executed during the CI
* `Makefile` - small Makefile that enables easier building and pushing of a new CI docker image

*Note that we run the CI on the GCP using [Cloud Build](https://cloud.google.com/build).
The docker image is directly referenced in the [cloudbuild.yaml](../cloudbuild.yaml), [cloudbuild-3_11.yaml](../cloudbuild-3_11.yaml), , [cloudbuild-4_0.yaml](../cloudbuild-4_0.yaml) and [cloudbuild-dse.yaml](../cloudbuild-dse.yaml) files which represent the jobs we do during the CI.*

## Updating the docker image

### Prerequisites

In order to push the new docker image to the GCP image registry `gcr.io/stargateio`, besides having permissions to do so, you should authenticate and setup your docker configuration using:

```bash
gcloud auth login
gcloud auth configure-docker
```

### Process

* Perform wanted updates related to the image.
* Bump the `VERSION` variable in the [Makefile](Makefile).
**This step is mandatory, otherwise you will overwrite the existing image.**
* Run `make info` and confirm that the image you are about to build and push has a correct tag.
* Run `make build push`.
It will build the image and push to the GCP repository.
* Update all the `cloudbuild*.yaml` files with the new version of the image.
* Commit and create a pull request.
Note that the created pull request will run the CI using the new image.
