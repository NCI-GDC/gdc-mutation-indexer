echo "$QUAY_PASSWORD" | docker login -u "$QUAY_USERNAME" --password-stdin quay.io
docker pull docker/dockerfile:1.0-experimental
docker pull docker.io/docker/dockerfile-copy:v0.1.9
bash -e docker/docker_push.sh --push