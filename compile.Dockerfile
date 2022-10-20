FROM eclipse-temurin:11-jdk-alpine AS build
WORKDIR /usr/src/app


RUN apk add --no-cache curl tar bash procps

ARG MAVEN_VERSION=3.8.6
ARG USER_HOME_DIR="/root"
ARG SHA=f790857f3b1f90ae8d16281f902c689e4f136ebe584aba45e4b1fa66c80cba826d3e0e52fdd04ed44b4c66f6d3fe3584a057c26dfcac544a60b301e6d0f91c26
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"
COPY pom.xml pom.xml
COPY sqrl-core ./sqrl-core
RUN mvn package -DskipTest=true

FROM eclipse-temurin:11-jdk-alpine
WORKDIR /usr/src/app
COPY --from=build /usr/src/app/sqrl-core/target/sqrl-core-1.0-SNAPSHOT.jar /usr/src/app
ENTRYPOINT ["java", "-cp", "sqrl-1.0-SNAPSHOT.jar", "ai.datasqrl.compile.Compiler"]