How to release:

Step 1: Assure you are in a clean working main
- git checkout main
- git pull

Step 2: Run the maven release
mvn --batch-mode release:clean release:prepare -DskipTests -Darguments=-DskipTests

Step 3: Go to github and create a new release with the existing tag