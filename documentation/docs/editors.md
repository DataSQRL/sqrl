# Editor Support

The SQRL language server brings the DataSQRL compiler into your editor. It is the [`lsp` command](compiler#lsp-command) of the DataSQRL CLI and works with any editor that supports the Language Server Protocol (LSP).

## Features

- Errors and warnings as you type, from the same compiler as the [`compile` command](compiler#compile-command), also for package files and GraphQL schemas
- Completion of tables, columns, functions, imports, hints, and template variables
- Documentation of tables, columns, and functions on hover, the signature of a function as you type its arguments, and the types of computed columns inline
- Go to definition, workspace symbols, find references, and highlighting of the occurrences of a name
- Renaming of tables, functions, and relationships, also in the GraphQL schema
- Quick fixes for common errors
- Semantic highlighting of tables, functions, relationships, and columns
- The outline of a script
- Code lenses above test tables that run the tests of the project

## IntelliJ IDEA

The DataSQRL plugin for IntelliJ IDEA 2026.1 and later starts the language server for the files of DataSQRL projects, and adds:
- Syntax highlighting of scripts
- Completion, documentation, and validation of package files with their JSON schema
- Running the tests of a project from the code lenses above its test tables, in the Run tool window
- The choice of the package files of a script, when several name it as their main script, in the DataSQRL item of the language services in the status bar

### Installation

Build the plugin in the `sqrl-intellij-plugin` directory of the [DataSQRL repository](https://github.com/DataSQRL/sqrl), which needs JDK 21.
Pass the version of DataSQRL that you use, so that the plugin starts the Docker image of that version by default:

```bash
./gradlew buildPlugin -PsqrlVersion=<version>
```

Then install `build/distributions/sqrl-intellij-plugin-<version>.zip` with **Settings | Plugins | ⚙ | Install Plugin from Disk…**.

### Configuration

In **Settings | Languages & Frameworks | DataSQRL**, choose how the plugin starts the language server:
- **Docker image of the CLI**, the default, which needs Docker. The image defaults to the version of DataSQRL that the plugin was built for.
- **Jar of the CLI**, see [From the Jar](#from-the-jar). The Java executable defaults to the one of `JAVA_HOME`, else the runtime of the IDE, which may be too new for the CLI. On Windows, the plugin starts the language server only from the jar.

Tests always run in the Docker image, which has the services that tests need, e.g. Kafka and Postgres. With the jar setting, the image runs the configured jar.

## Other Editors

Configure your editor to start the language server for the files of DataSQRL projects:
- SQRL scripts (`.sqrl`), with the language ID `sqrl`
- Package files, the JSON files with `package` in their name, with the language ID `json`
- GraphQL schemas and operations (`.graphqls` and `.graphql`), with the language ID `graphql`

The server answers requests about scripts, and reports the errors of the package files and GraphQL schemas that the editor sends to it.

### With Docker

```bash
docker run --interactive --rm --tmpfs /workspace \
  --volume /path/to/project:/path/to/project \
  datasqrl/cmd lsp
```

The server reads files at the paths that the editor sends, so mount the directory of your project at the same path in the container.
If the project includes packages from outside its directory, mount a directory that contains them as well.
The image requires a `/workspace` directory, and a temporary file system there keeps the files that the server's checks create out of your project.

On Windows, the paths of the editor are not paths in the container, so run the server from the jar instead.

### From the Jar

The server also runs from the jar of the CLI with Java 17 to 22, since Hadoop, which connectors such as Iceberg use, fails on Java 23 and later.
Copy the jar out of the Docker image:

```bash
id=$(docker create datasqrl/cmd)
docker cp "$id:/opt/sqrl/sqrl-cli.jar" sqrl-cli.jar
docker rm "$id"
```

The CLI needs the JVM options of the image, which this command prints:

```bash
docker run --rm --entrypoint printenv datasqrl/cmd SQRL_JVM_TOOL_OPTS
```

Start the server with those options:

```bash
java <JVM options> -jar sqrl-cli.jar lsp
```

The server writes its log to stderr, which most editors show in their language server log.

## How the Server Finds a Project

The server checks a script with the package files of its project, as you would pass them to the [`compile` command](compiler#compile-command). It finds them by their content, so you can name them freely:
- Package files are the JSON files with `package` in their name that have sections of a DataSQRL package file, e.g. `script` or `engines`.
- A base package file names the main script in `script.main`. An overlay has no main script and adds to a base whose name without `.json` it starts with, e.g. `package-test.json` adds to `package.json`. For a base with `shared` in its name, the part before `shared` counts, e.g. `orders-test-package.json` adds to `orders-shared-package.json`.
- The project of a script is the base that names it as its main script, or else a base in the directory of the script or the nearest one above it, since the script is then imported by a main script.
- If several bases qualify, the server prefers a base for local runs, with `local` in its name, since it needs no cloud services, and then a base for tests, with `test` in its name, since scripts are checked with their test tables.
- The base gets its overlay for tests, or else for development, with `test` or `dev` in its name.

You can select other package files for a script with the `selectedPackageFiles` setting, or name the package files of all projects with the `packageFiles` setting.

## Settings

The editor passes the settings as `initializationOptions` when it starts the server, and can change them with `workspace/didChangeConfiguration`:

```json
{
  "packageFiles": ["package.json", "package-test.json"],
  "checkDelayMillis": 300,
  "fullCompileOnSave": true,
  "selectedPackageFiles": {
    "file:///home/me/shop/shop.sqrl": [
      "file:///home/me/shop/shop-package-local.json",
      "file:///home/me/shop/shop-package-local-test.json"
    ]
  }
}
```

| Setting                | Default         | Description                                                                                                                                                                                                                                         |
|------------------------|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `packageFiles`         | found by content | The package files of every project, relative to the project root, if they are named the same in all projects. The project of a script is then in its directory or the nearest one above it with the first of these files.                              |
| `checkDelayMillis`     | `300`           | How long the server waits after an edit before it checks, so that typing does not trigger a check for every keystroke.                                                                                                                             |
| `fullCompileOnSave`    | `true`          | Whether the server runs the full compilation when a file is saved, which also validates the API, e.g. the GraphQL schema, but takes longer.                                                                                                       |
| `selectedPackageFiles` | none            | The package files that the user selected for a main script, from the URI of the script to the URI of a base or an array of the URIs of a base and its overlays. This is for a script that several bases name as their main script, e.g. one for local runs and one for tests. |

## Commands

The server and the editor exchange two commands, which editor plugins use:
- `sqrl.packageFiles`, which the server executes: its argument is the URI of a script, and it returns the URIs of the package files of the script. `candidates` are the bases that name the script as their main script, `overlays` are the overlays of the base in use, and `used` are the package files that the script is checked with, the base first. Use it to let the user select package files for `selectedPackageFiles`.
- `sqrl.runTests`, which the editor handles: it is the command of the code lenses above test tables, and its arguments are the URIs of the package files of the project. Run the [`test` command](compiler#test-command) with those package files in the directory of the first one.
