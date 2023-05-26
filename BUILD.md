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