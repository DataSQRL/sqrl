How to release:

Step 1: Assure you are in a clean working main
- git checkout main
- git pull

Prerelease:
Update the build.gradle.ftl in the profiles to point to the new sqrl version
```
    sqrlVersion = "0.?"
```

Update the Dockerfile in the vertx profile:
```
FROM datasqrl/sqrl-server:v.0?
```

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

Post release profile update:
Update Dockercompose in flink profiles to point to the new version.
```
FROM datasqrl/sqrl-dependencies:0.? as m2-dependencies
```

Update the build.gradle.ftl to point to the new snapshot
```
    sqrlVersion = "0.?-SNAPSHOT"
```

Doc updates:
Update the playbook to include the new dependencies

Build updates:
If updating to an RC branch, add it to the build-config.yml branches.


Step 5: Test the SQRL command on something!