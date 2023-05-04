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

Run `mvn package -DskipTests` first.

Build docker image for the DataSQRL command (run in sqrl-tools/sqrl-cli directory):

```bash
docker build -t datasqrl/cmd . 
```