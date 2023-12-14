
# Custom User-Defined Functions in Flink
This document serves as a guide for developers who are looking to create and deploy custom user-defined functions in Apache Flink. We will specifically focus on a function called MyScalarFunction, designed to double the values of input numbers, demonstrating the implementation and deployment of a scalar function within the Flink framework.

## Introduction
User-defined functions (UDFs) in Flink are powerful tools that allow for the extension of the system's built-in functionality. UDFs can be used to perform operations on data that are not covered by the built-in functions. This guide will lead you through the process of creating, compiling, and deploying a UDF.

## Creating a User-Defined Function
1. **Project Structure:** The myjavafunction folder contains a sample Java project, demonstrating the structure and necessary components of a Flink UDF.

2. **Defining the Function:** The main component of this project is the MyScalarFunction class. This class should extend either ScalarFunction or AggregateFunction based on your requirement.

3. **ServiceLoader Entry:** The function must be registered with a ServiceLoader entry. This is essential for DataSQRL to recognize and use your UDF.
- **AutoService Library:** The example includes the AutoService library by Google, simplifying the creation of ServiceLoader META-INF manifest entries.

## Compilation and Packaging
1. **SQRL Compilation:** Compile the project using Datasqrl's command interface, which prepares your script for deployment in the Flink environment.

```shell
docker run --rm -v $PWD:/build datasqrl/cmd compile myudf.sqrl --mnt $PWD
```

2. **Alternate Packaging:** Next, you need to package your jar with your custom library using a more expensive jar packager. This will rebundle your library with the flink jar. If you ship your jar to flink directly, you can skip this step. This step can take some time to compile.
```shell
docker run --rm -v $PWD/build:/build datasqrl/engine-flink:latest
```

## Deployment and Testing
### Starting Docker
1. **Docker Environment:** Deploy your new flink jar using the docker-compose example in the deploy folder.

```shell
(cd build/deploy; docker-compose up
```
### Creating and Testing Records
1. Creating a Record: Test the function by creating a record via a GraphQL query.
```shell
curl -X POST 'http://localhost:8888/graphql' \
    -H 'Content-Type: application/graphql' \
    -d '
mutation {
  entry(input: { val: 2 }) {
    val
  }
}'
```

2. Verifying Function Execution: Confirm the function's execution and output with another GraphQL query. You should see two values come back, 2 and 4.

```shell
curl -X POST 'http://localhost:8888/graphql' \
    -H 'Content-Type: application/graphql' \
    -d '
query {
  myTable {
    val
    myFnc
  }
}'
```