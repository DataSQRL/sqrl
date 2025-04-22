# How to Release

## Pre-release
### 1. Notify you are releasing
Announce that you are releasing in the #engineering channel

### 2. Assure you are in a clean working main
```
git checkout main
git pull
```

### 3. Update release versions
We need to update some datasqrl version numbers prior to release.

**Note:**  Be aware some versions have a 'v' prefix and others do not.
Update the following files:
```
- profiles/default/README.md : 1 place under 'datasqrl.profile.default'
- profiles/default/package.json : 3 places under sqrl-version, sqrl-vertx-image, and under 'package'
- sqrl-tools/sqrl-cli/src/main/java/com/datasqrl/cmd/RootCommand.java : 1 place under version
- sqrl-tools/sqrl-packager/src/main/java/com/datasqrl/cmd/PackageBootstrap.java : 1 place under DependencyImpl
- sqrl-tools/sqrl-run/gradle-deps/build.gradle : 1 place under sqrlVersion
```

### 4. Push commit
Create a commit under main with the message `Prepare for v0.?.?` and push.

## Release SQRL
### 1. Run the maven release command
Note: Update the release version on the command below. This will trigger the git release processes and create tag.
Note: the mvn release plugin uses the http github url defined in the SCM part to push. Please use you github personal access token to log in while pushing.
`
mvn --batch-mode release:clean release:prepare -DreleaseVersion=0.?.? -DskipTests -Darguments=-DskipTests
`

### 2. Deploy the flink maven packages to Sonatype

#### Pre-step

Before you can release the maven packages, you must log into sonatype and configure maven.
- Go to https://us-west-2.console.aws.amazon.com/secretsmanager/listsecrets?region=us-west-2 to retrieve the SonatypeAuth username and password AND SonatypePomToken
- Go to https://central.sonatype.com/ and sign in with the username and password
- Go to your ~/.m2/settings.xml and add the servers listed in the SonatypePomToken
- You must also have your GPG key configured

#### To release:
```
git checkout tags/v0.?.?
cd sqrl-flink-lib
mvn deploy  
```

After it completes, go to [https://central.sonatype.com/publishing](https://central.sonatype.com/publishing) and go to 'Publish' to publish the new maven libraries.

## Release SQRL Default Profile
### Prestep: Log in to sqrl repo
```
sqrl login
```

### 1. Upload default profile to SQRL
Go to `profiles/default` and run the release:
```
sqrl publish
```

## Release the CLI
### 1. Rebuild the project
You must rebuild all of sqrl.
```bash
mvn clean package -DskipTests
```

### 2. Prepare jar
Go to `sqrl-tools/sqrl-cli/target` and rename `sqrl-cli.jar` to `sqrl-cli-v0.?.?.jar`.

### 3. Record sha256 checksum
Run `shasum -a 256 sqrl-cli-v0.?.?.jar` on this file and record the result, we will use this has in a later step.

### 4. Upload to aws
Go to aws in the datasqrl account under the `us-west-2` region. Go to S3 and the `sqrl-cmd` bucket. Upload the `sqrl-cli-v0.?.?.jar` and under `permissions`, set it `Grant public-read access`.

### 5. Checkout out brew project
If you have not already, clone the repo:
```
git clone git@github.com:DataSQRL/homebrew-sqrl.git
```
### 6. Update `sqrl-cli.rb`
Taking note of which versions have a 'v' prefix, update the versions in the file. Add the sha256 checksum. 

**Note**: Before you push, assure that the sonatype jars have been released and the release github action is completed. Go to [https://hub.docker.com/orgs/datasqrl/repositories](https://hub.docker.com/orgs/datasqrl/repositories) to assure that it has been released.

### 7. Commit and push
```
git commit -sam `Bump to v0.?.?`
git push
```

## Add release to github releases
### 1. Create a new release
Go to [https://github.com/DataSQRL/sqrl/releases](https://github.com/DataSQRL/sqrl/releases) and click 'Draft a new release'. Choose the tag of the version you just published.

## Verify the release
### 1. Update the brew command on your local machine:
```
brew tap datasqrl/sqrl && brew install sqrl-cli
```

**Note**: If you had an issue with the release, you must uninstall and untap.
```
brew uninstall sqrl-cli && brew untap datasqrl/sqrl
```

### 2. Verify the version
```bash
sqrl --version 
```

### 3. Go through the 'metrics' example
Go to the main readme.md and run through the metrics example to assure it still works as intended.

### 4. Announce finsihed release
Go to the #engineering channel and announce that you are finished with the release.
