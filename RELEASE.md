# Release Process for SQRL

Releasing a new version of `sqrl` is automated and streamlined via CircleCI.
The project follows semantic versioning, and have separate release branches for every minor version (e.g. `release-1.0`, `release-1.1`).
The `main` branch points to the next upcoming minor (or major) version.

## Release Steps

1. If you release a patch version, make sure all the necessary commits are present on the relevant `release-x.y` branch
   If you release a new minor or major version, first update `main`:
   - [`SECURITY.md`](SECURITY.md): list `x.y.x` as supported, and `< ` the oldest supported version as unsupported
   - [`documentation/docs/compatibility.md`](documentation/docs/compatibility.md): add an `x.y.x` row to the compatibility table

   The [Release docs](.github/workflows/release-docs.yml) workflow fails while either file does not cover the latest release line,
   or while the Flink, Postgres or Iceberg versions in the compatibility table row of a supported version differ from its release.
   On `main` it checks these rows against the latest release tag of each version, on `release-x.y` branches and release tags against their own build files.
   So if a patch release bumps one of these versions, update the row on its `release-x.y` branch and on `main`.
2. Create a new signed **tag** using [SemVer](https://semver.org/) (for example `1.0.0`, or `1.0.0-alpha.1`)
   ```sh
   git tag -s 1.0.0 -m 'Release version 1.0.0'
   git push origin 1.0.0
   ``` 
3. Go to the [GitHub Releases page](https://github.com/DataSQRL/sqrl/releases/new)
4. Fill in the release title and changelog (optional but recommended)
5. Click **"Publish release"**
6. In case of a new minor or major release, create a new `release-x.y` branch for the new version from `main` that points to the tagged commit

Once published, CircleCI will automatically build and publish the artifacts.

## What Gets Released

CircleCI will handle all of the following:

- 📦 **Maven artifacts** → [Maven Central](https://repo1.maven.org/maven2/com/datasqrl/sqrl-root/) and [GitHub Packages](https://github.com/orgs/DataSQRL/packages?repo_name=sqrl), which still gets them when Maven Central rejects a release
- 🐳 **Docker image** → [`datasqrl/cmd` on Docker Hub](https://hub.docker.com/r/datasqrl/cmd)
- 📥 **CLI `.jar` files** → included in GitHub Releases

## Notes

- The **tag name** defines the version across all systems.
- Make sure the tag string strictly follows SemVer.
- No need to push code changes or update version files manually.
- Releases typically complete within a few minutes.

## Troubleshooting

If anything fails:
- Check the [CircleCI jobs](https://app.circleci.com/pipelines/github/DataSQRL/sqrl).
- Retry the failed job if the error was transient.
- Re-running the `deploy` job repeats both Maven deploys, so it fails on the one that succeeded before:
  GitHub Packages rejects release files it already has with 409, and Maven Central rejects a version it already published.
  The other one still gets deployed. If GitHub Packages failed halfway, delete the versions it already uploaded before re-running.
- If you must delete a release, remove the tag both from GitHub and locally:  
  ```bash
  git tag -d <tag>
  git push origin :refs/tags/<tag>
