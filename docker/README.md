
### 1. Confirm release version
When releasing the new version, the release manager will verify the image tag and push the image to the image repository,
The latest image tag will be written to [docker-compose.yaml](./docker-compose.yaml)，users also can independently verify whether the version of the StreamPark image in the [docker-compose.yaml](./docker-compose.yaml) file is correct (If the current branch has not been released, the image tag is the last release image tag):

```yaml
version: '3.8'
services:
    streampark:
        image: apache/streampark:2.1.7
```

### 2. docker-compose up

```shell
docker-compose up -d
```

### 3. open in browser

http://localhost:10000

#### [more detail](https://streampark.apache.org/docs/get-started/docker-deployment)
