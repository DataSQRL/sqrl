# Contributing to DataSQRL

Thanks for your interest in DataSQRL! We welcome contributions from anyone: code, documentation,
bug reports, and ideas.

By participating, you agree to follow our [Code of Conduct](CODE_OF_CONDUCT.md).
To report a security vulnerability, follow the [Security Policy](SECURITY.md) and do not open a public issue.

## How to Contribute

### 1. Discuss

- **Questions and ideas**: Start a thread in [GitHub Discussions](https://github.com/DataSQRL/sqrl/discussions).
  For new features or larger changes, please discuss the approach there first, so we can agree on
  the direction before you invest time in an implementation.
- **Bugs and agreed-upon work**: Search the [GitHub Issues](https://github.com/DataSQRL/sqrl/issues)
  to see if it is already reported. If not, open a new issue with a clear description, steps to
  reproduce, and the DataSQRL version you are using.

Small fixes, such as typos or obvious bugs, can go straight to a pull request.

### 2. Get Assigned

Comment on the issue you want to work on, so others know it is being worked on and we can avoid duplicate effort.

### 3. Implement

1. Fork the repository and create a branch from `main`.
2. Make your change, following the code style and conventions described in [AGENTS.md](AGENTS.md).
3. Add or update tests that cover your change. Use the `given_when_then` naming pattern and AssertJ assertions.
4. Format the code and make sure the build passes locally (see [Development Setup](#development-setup)).
5. [Sign off and sign](#sign-your-work) your commits.

### 4. Open a Pull Request

- Reference the related issue in the description (e.g. `Closes #123`).
- Use a [Conventional Commits](https://www.conventionalcommits.org) title. This is enforced by CI.
  Allowed types: `feat`, `fix`, `chore`, `test`, `docs`, `refactor`, `ci`, `build`, `perf`, `revert`
  (e.g. `feat: Add support for Iceberg sink`).
- Keep pull requests focused on a single change. Unrelated changes belong in separate pull requests.
- Make sure all CI checks pass.

### 5. Review and Merge

A contributor or committer will review your pull request and may ask for changes or more
information. Once it is approved and CI is green, a committer will merge it. Fixes may be
backported to the supported `release-*` branch when applicable.

## Development Setup

### Prerequisites

- Java 17
- Maven 3
- Docker (required for integration and container tests)

### Build

Run a full build first, and let all tests run:

```bash
mvn clean install
```

This is required for development, because it installs the test and dev artifacts that other modules
depend on into your local `~/.m2` repository. Re-run it whenever you change dependencies or before
running a Maven goal inside a module subdirectory.

The Docker images (`datasqrl/cmd`, `datasqrl/sqrl-server`) are built as part of the `package` phase.

### Useful Commands

```bash
# Quick build: skip tests and checks
mvn clean install -P quickbuild

# Format code (Google Java Format)
mvn -P dev initialize

# Unit tests of a single module
mvn test -pl sqrl-planner

# Integration tests (requires Docker)
mvn verify

# Update test snapshots after an intentional change in compiler output
mvn clean install -P update-snapshots
```

### Tips

- Use `-DskipTests` instead of `-Dmaven.test.skip=true`. The former still builds the test JARs,
  which downstream modules need to resolve their dependencies.
- Add `-T <threads>` (e.g. `-T 1C`) to build modules in parallel.
- On case-insensitive file systems (macOS, Windows), run `git config core.ignorecase false` so Git
  picks up file renames that only change letter casing.

## Sign Your Work

The _sign-off_ is a simple line at the end of the message for a commit. All commits need to be signed.
Your signature certifies that you wrote the patch or otherwise have the right to contribute the material
(see [Developer Certificate of Origin](https://developercertificate.org)):

```
This is my commit message

Signed-off-by: John Doe <john.doe@example.com>
```

Git has a [`-s`](https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---signoff) command line option to
append this automatically to your commit message:

```bash
$ git commit -s -m "This is my commit message"
```

Unfortunately, anyone with write access to a repository can easily impersonate another user.
Consider how each commit is associated with a user via their email address.
There's nothing stopping someone from using someone else's email address to make commits.
The issue extends to the signoff message as well.

That's why it's advisable to sign your commits with a unique key. Git offers support for various types of keys,
and this time, we'll walk you through signing your commits using GPG.

#### Setup Git using GPG

Ensure that gpg is installed on your system.

On MacOS:
```bash
brew install gpg
```

Generate your key:
```bash
gpg --full-generate-key
```

Recommended settings:
- **key kind:** (1) RSA and RSA
- **key size:** 4096
- **key validity:** key does not expire
  (you can revoke keys, so unless you don't lose access to your key it is more convenient)
- **real name:** it is recommended using your real name
- **email address:** it is recommended to use the same email address here that you use to commit your work
- **comment:** it is recommended to use different keys for different use-cases / organizations.
  If you use the same email across organizations, you can distinguish your keys with the help of this field.
  eg.: "CODE SIGNING KEY" or "DATASQRL CODE SIGNING KEY"

You can create the new key by selecting "(O)kay"

To view your key, you issue this command:
```bash
gpg --list-secret-keys --keyid-format=long
```

The output should look like this:
```
sec   rsa4096/D2A162EAE1016F3G 2024-04-05 [SC]
      AFB8C2DEFEA93470D81C84E7D2A162EAE1016F3G
uid                 [ultimate] John Doe (CODE SIGNING KEY) <john.doe@example.com>
ssb   rsa4096/2F7B9EAC4D6F8150 2024-04-05 [E]
```

To use the above key to sign your commits cd into a repository and issue these commands:
```bash
git config user.signingkey D2A162EAE1016F3G
git config commit.gpgsign true
```

You also need to add the public key to your github profile for the signing to be verified.

To do so, go to your github settings page, select the `SSH and GPG keys` tab.

Press `New GPG Key`, then enter a name for the key and the outputs of the following command.

```
gpg --armor --export D2A162EAE1016F3G
```

## Contributors

Contributors for this project are documented in the project's [CONTRIBUTORS](CONTRIBUTORS.md) file.

## License

By contributing to DataSQRL, you agree that your contributions will be licensed under the
[Apache 2.0 License](LICENSE).
