# SQRL Tools

Contains the tools that compose the compiler:

* CLI: The command line interface for DataSQRL implemented in picocli
* Config: Utility classes for interacting with the package.json configuration files that DataSQRL uses as build manifests
* Packager: Reads the configuration, instantiates the build directory, resolves dependencies, and runs preprocessors
* Run: Runs generated DataSQRL pipelines
* Test: Executed DataSQRL tests against running DataSQRL pipelines
* Discovery: A preprocessor which automatically produces table configurations for jsonl and csv files