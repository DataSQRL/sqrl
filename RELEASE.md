How to release:

Step 1: Assure you are in a clean working main
- git checkout main
- git pull

Step 2: Run the command to update the POM's, add a tag, and push the new commit.
Note: Update the release version on the command below
mvn --batch-mode release:clean release:prepare -DreleaseVersion=0.?.0 -DskipTests -Darguments=-DskipTests

Step 3: Go to github and go to 'Tags'. Create a new release on the newly created tag.

Step 4: Go to docker hub and assure the new tag exists:
https://hub.docker.com/repositories/datasqrl

Step 5: Test the SQRL command on something!