## Getting started with development
First run `mvn clean install` and let all the tests run. This is
required for development since it'll install the relevant test
and dev poms to your local .m2 folder. Run clean install whenever
you make dependency changes and are trying to do a maven goal inside
a maven subdirectory.

## How to build Parser in Intellij

1. Run sqrl-calcite > Plugins > antlr4:antlr4
2. Refresh maven dependencies

## Helpful commands while developing

`mvn -Dmaven.test.skip compile jar:test-jar install`

## Git case-insensitivity
`git config core.ignorecase false`

### Maven
-DskipTests=true and not -Dmaven.test.skip=true

skipTests will build the test jars which is required for downstream module pom resolution

Add the -T parameter for more threads. Modules can compile in parallel.


## Build docker

To build the docker image for the DataSQRL command:

```bash
docker build -t datasqrl/datasqrl-cmd .
```

# To run int test:

Build the docker server image:
```
mvn package
cd sqrl-server/sqrl-server-vertx
docker build . -t datasqrl/sqrl-server:latest
```
If you want to skip the unit tests, add `-DskipTests=true`
Go back to the root directory.

Third, run the integration tests:
```
mvn verify
```

