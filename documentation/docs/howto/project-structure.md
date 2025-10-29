# Project Structure

A DataSQRL project is structured as follows where `{name}` is the project name.

```
├── {name}.sqrl                         # Contains the main data processing logic
├── {name}-[run/test/prod]-package.json # Configuration files for running locally, testing, and deploying the project
├── {name}-connectors/                  # Contains source and sink table definitions, shared connector logic, schemas, and data files
│   └── sources-[run/test/prod].sqrl    # Contains the source table definitions, split by variant or environment
├── snapshots/                          # Contains the snapshot data for tests
│   └── {name}/                         # One directory per project
├── {name}-api/                         # Contains the API schema and operation definitions for the project
│   ├── schema.v1.graphqls              # GraphQL schema definition
│   ├── tests/                          # Contains GraphQL test queries as .graphql files
│   └── operations-v1/                  # Contains any operation definitions as .graphql files
└── README.md                           # Explain the project(s) and structure
```

A project has one or more `package.json` configuration files to configure the compiled pipeline for different environments: running locally, testing, and one or more deployment environments.
The targeted environment is used in the name, e.g. `run`, `test`, `qa`, `prod`, etc.

The `package.json` file is the authoritative source that defines the main SQRL script and (optional) GraphQL schema and operations.
It also configures snapshot and test directories. Always consult the `package.json` files for the relative file paths to the project source files.

For advanced project or when multiple projects share one directory, the structure may include:
```
├── [shared/authentication]-package.json # Config file that is shared across projects
├── tests/                               # Folder that contains test code to separate it from the main logic
│   └── {test-name}.sqrl                 # This file is included inline in the main script
├── functions/                           # User defined functions
└── shared.sqrl                          # SQRL script that's shared across projects
```