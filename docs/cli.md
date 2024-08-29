import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


# DataSQRL Command

The DataSQRL command compiles, runs, and tests SQRL scripts. It also provides utilities for managing data sources and sinks, uploading packages to the repository, and other convenience features.

You invoke the DataSQRL command in your terminal or command line. Choose your operating system below or use Docker which works on any machine that has Docker installed.

## Installation

<Tabs groupId="cli">
<TabItem value="Mac" default>

```bash
brew tap datasqrl/sqrl
brew install sqrl-cli
```

:::note
Check that you're on the current version of DataSQRL by running `sqrl --version`
To update an existing installation:

```bash
brew upgrade sqrl-cli
```
:::

</TabItem>
<TabItem value="Docker">
Always pull the latest Docker image to ensure you have the most recent updates:

```bash
docker pull datasqrl/cmd:latest
```

:::note
The Docker version of DataSQRL has limited functionality and does not support the development or testing runtime that ships with the DataSQRL command. These significantly speed up the development cycles from minutes to seconds.
:::

</TabItem>
</Tabs>

### Global Options
All commands support the following global options:

|Option/Flag Name	|Description|
|--------------|---------------|
|-c or --config|	Specifies the path to one or more package configuration files. Contents of multiple files are merged in the specified order. Defaults to package.json in the current directory, generating a default configuration if none exists.|

Note, that most commands require that you either specify the SQRL script (and, optionally, a GraphQL schema) as command line arguments or use the
`-c` option to specify a project configuration file that configures the SQRL script (and, optionally, a GraphQL schema).

## Compile Command
The compile command processes an SQRL script and, optionally, an API specification, into a deployable data pipeline. The outputs are saved in the specified target directory.


<Tabs groupId="cli">
<TabItem value="Mac" default>

```bash
sqrl compile myscript.sqrl myapischema.graphqls
```

</TabItem>
<TabItem value="Docker">

```bash
docker run --rm -v $PWD:/build datasqrl/cmd compile myscript.sqrl myapischema.graphqls
```
</TabItem>
</Tabs>

|Option/Flag Name|	Description|
|--------------|---------------|
|-a or --api	|Generates an API specification (GraphQL schema) in the file schema.graphqls. Overwrites any existing file with the same name.|
|-t or --target	|Directory to write deployment artifacts, defaults to build/deploy.|
|--profile|	Selects a specific set of configuration values that override the default package settings.|


The command compiles the script and API specification into an integrated data product. The command creates a `build` with all the build artifacts that are used during the compilation and build process (e.g. dependencies). The command writes the deployment artifacts for the compiled data product into the `build/deploy` directory. Read more about deployment artifacts in the deployment documentation.


## Test Command

The test command executes the provided test queries and all tables annotated with `/*+test */` and snapshots the results.

When you first run the test command, it will create the snapshots and fail. All subsequent runs of the test command compare the results to the previously snapshotted results and succeed if the results are identical, else fail.

<Tabs groupId="cli">
<TabItem value="Mac" default>

```bash
sqrl test myscript.sqrl myapischema.graphqls
```

</TabItem>
<TabItem value="Docker">

```bash
docker run --rm -v $PWD:/build datasqrl/cmd test
```
</TabItem>
</Tabs>

Options for the Test Command:

|Option/Flag Name| 	Description                                         |
|--------------|------------------------------------------------------|
|-s or --snapshot| 	Path to the snapshot files. Defaults to `snapshot`. |
|--tests| 	Path to test query files. Defaults to `tests`.      |


## Publish Command
Publishes a local package to the repository. It is executed from the root directory of the package, archiving all contents and submitting them under the specified package configuration. The package must have a main `package.json` that contains the package information:

```json
{
  "version": "1",
  "package": {
    "name": "myorg.mypackage",
    "version": "0.1.2",
    "variant": "dev",
    "description": "This is my profile",
    "homepage": "http://www.mypackage.myorg.com",
    "documentation": "More information on my package",
    "topics": [ "mytag" ]
  }
}
```


<Tabs groupId="cli">
<TabItem value="Mac" default>

```bash
sqrl publish --local
```

</TabItem>
<TabItem value="Docker">

```bash
docker run --rm -v $PWD:/build datasqrl/cmd publish --local
```
</TabItem>
</Tabs>

|Option/Flag Name|	Description|
|--------------|---------------|
|--local	|Publishes the package to the local repository only.|

### How repository resolution works

A repository contains DataSQRL packages. When compiling an SQRL script, the DataSQRL compiler retrieves dependencies declared in the [package configuration](/docs/reference/sqrl/datasqrl-spec) and unpacks them in the build directory.

The remote DataSQRL directory is hosted at [https://dev.datasqrl.com](https://dev.datasqrl.com). Packages in the remote repository can be retrieved from any machine running the DataSQRL compiler with access to the internet.

DataSQRL keeps a local repository in the hidden `~/.datasqrl/` directory in the user's home directory. The local repository is only accessible from the local machine. It caches packages downloaded from the remote repository and contains packages that are only published locally.


## Login Command

Authenticates a user against the repository. A user needs to be authenticated to access private packages in the repository or to publish a package.

<Tabs groupId="cli">
<TabItem value="Mac" default>

```bash
sqrl login
```

</TabItem>
<TabItem value="Docker">

```bash
docker run --rm -v $PWD:/build datasqrl/cmd login
```
</TabItem>
</Tabs>
