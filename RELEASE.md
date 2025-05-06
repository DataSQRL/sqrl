# Release Process for SQRL

Releasing a new version of `sqrl` is automated and streamlined via CircleCI.

## ✅ Release Steps

1. Go to the [GitHub Releases page](https://github.com/DataSQRL/sqrl/releases/new).
2. Create a new **tag** using [Semantic Versioning (SemVer 2.0.0)](https://semver.org/) — for example:
   - `1.0.0`
   - `0.9.0-alpha.1`
   - `1.2.3+build.456`
3. Fill in the release title and changelog (optional but recommended).
4. Click **"Publish release"**.

Once published, CircleCI will automatically build and publish the artifacts.

## 🚀 What Gets Released

CircleCI will handle all of the following:

- 📦 **Maven artifacts** → [Maven Central](https://repo1.maven.org/maven2/com/datasqrl/sqrl-root/)
- 🐳 **Docker image** → [`datasqrl/cmd` on Docker Hub](https://hub.docker.com/r/datasqrl/cmd)
- 📥 **CLI `.jar` files** → included in GitHub Releases

## 📝 Notes

- The **tag name** defines the version across all systems.
- Make sure the tag string strictly follows SemVer.
- No need to push code changes or update version files manually.
- Releases typically complete within a few minutes.

## 🛠 Troubleshooting

If anything fails:
- Check the [CircleCI jobs](https://app.circleci.com/pipelines/github/DataSQRL/sqrl).
- Retry the failed job if the error was transient.
- If you must delete a release, remove the tag both from GitHub and locally:  
  ```bash
  git tag -d <tag>
  git push origin :refs/tags/<tag>
