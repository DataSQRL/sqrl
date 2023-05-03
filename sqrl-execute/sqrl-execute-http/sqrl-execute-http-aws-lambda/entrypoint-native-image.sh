#!bin/sh

cd /build
# TODO: fix validation by rerunning graalvm agent
native-image -jar /sqrl-execute-http-aws-lambda.jar -H:ConfigurationFileDirectories=/config-dir --static -H:IncludeResources=i18n/Validation.*

echo "done."
