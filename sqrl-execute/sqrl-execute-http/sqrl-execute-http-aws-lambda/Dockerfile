# Creates the native image executable
FROM ghcr.io/graalvm/graalvm-ce:ol7-java17-22.3.1

RUN gu install native-image
RUN yum update -y

COPY config-dir/ /config-dir/
# RUN yum install musl-devel -y
COPY target/sqrl-execute-http-aws-lambda.jar /sqrl-execute-http-aws-lambda.jar
COPY entrypoint-native-image.sh /entrypoint-native-image.sh

RUN chmod +x /entrypoint-native-image.sh
ENTRYPOINT ["sh","/entrypoint-native-image.sh"]
