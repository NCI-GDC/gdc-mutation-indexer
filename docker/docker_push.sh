#!/usr/bin/env bash

export DOCKER_BUILDKIT=1

PARAM=$1; shift
DEPLOY="no"
IMAGE_NAME="quay.io/ncigdc/gdc-mutation-indexer"

if [ "$PARAM" = "--push" ]; then
  DEPLOY="yes";
fi

if [ -z ${TRAVIS_BRANCH+x} ]; then
  GIT_BRANCH=$(git symbolic-ref --short -q HEAD);
else
  GIT_BRANCH=$TRAVIS_BRANCH;
fi

# replace slashes with underscore
GIT_BRANCH=${GIT_BRANCH/\//_}

# keep track of tags for pushing
TAGS=("$IMAGE_NAME:$GIT_BRANCH")

# initial build command
BUILD_COMMAND=(build --ssh default -t "$IMAGE_NAME:$GIT_BRANCH")

# add latest tag if branch is master
if [ "$GIT_BRANCH" = "master" ]; then
  TAGS+=("$IMAGE_NAME:latest")
  BUILD_COMMAND+=(-t "${TAGS[1]}")
fi

docker "${BUILD_COMMAND[@]}" .

if [ "$DEPLOY" = "yes" ]; then
  for tag in "${TAGS[@]}"; do
    docker push "$tag";
  done
fi