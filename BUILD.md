# Build

## Maven

```bash
mvn clean install
```

To re-generate the snapshots add

```bash
-P update-snapshots
```

If tests fail due to Flink memory or buffer issues, try uncommenting the configuration
line in `ExecutionEnvironmentFactory.java`.

## Build Docker

Check out the `.github/workflows/docker.yml` for build steps.

## TroubleShooting

```bash
docker: Cannot connect to the Docker daemon at unix:///Users/matthias/.docker/run/docker.sock. Is the docker daemon running?

```

If you are trying to run the build or individual tests on Mac and get the error that TestContainers could not find a docker environment even though you are running Docker, then execute the following command:
```bash
sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock
```
See [StackOverflow](https://stackoverflow.com/questions/61108655/test-container-test-cases-are-failing-due-to-could-not-find-a-valid-docker-envi) for more information.