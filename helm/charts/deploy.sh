AUTOMATE_DOCKERFILE_PATH=${DOCKERFILE_PATH:-"../../"}
REPO_SLUG=${REPO_SLUG}

docker build -t hedra:latest \
 --no-cache \
 --build-arg REPO_BRANCH=main \
 --build-arg REPO_SLUG=$REPO_SLUG \
 --target=run \
  ${DOCKERFILE_PATH}

helm install performance-testing-dev performance-testing --values ../vars/values.yaml