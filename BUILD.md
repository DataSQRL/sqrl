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

## Docker

Build docker image for the DataSQRL command:

```bash
docker build -t datasqrl/datasqrl-cmd . 
```