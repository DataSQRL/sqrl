How to release:

Step 1: Assure you are in a clean working main
- git checkout main
- git pull

Prerelease:
Update the build.gradle.ftl and package.json in the profiles to point to the new sqrl version. Update the docker image references.
```
    sqrlVersion = "0.?"
```

Update the RootCommand version from 'v0.x.0'

Update the default dependency version in the cli from 'v0.x.0'

Commit and push this with a message "Bump version to prepare for v0.x release"

Step 2: Run the command to update the POM's, add a tag, and push the new commit.
Note: Update the release version on the command below
mvn --batch-mode release:clean release:prepare -DreleaseVersion=0.?.0 -DskipTests -Darguments=-DskipTests

Step 3: Check out the tag that was just created, release to sonatype:
```
git checkout v0.?
cd sqrl-flink-lib
mvn deploy  
```

(https://central.sonatype.com/publishing)[SonaType]

Log in as `dhenneberger` (for com.datasqrl) an publish the deployment


Step 3: Go to github and go to 'Tags'. Create a new release on the newly created tag.

Step 4: Go to docker hub and assure the new tag exists:
https://hub.docker.com/repositories/datasqrl

Step 5: Release the homebrew cli command:
- zip the sqrl-cli.jar and sqrl-run.jar and create a sha and update the homebrew-sqrl repo.

Step 6: Test the SQRL command on something!