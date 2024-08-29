FROM eclipse-temurin:11.0.24_8-jre-jammy
WORKDIR /usr/src/app
ARG POSTGRES_VERSION=15


# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install necessary packages
RUN apt-get update && \
    apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    software-properties-common

# Add Docker's official GPG key to the /usr/share/keyrings directory
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Set up the stable Docker repository
RUN echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update the package list again after adding Docker's repository
RUN apt-get update

# Install Docker Engine
RUN apt-get install -y docker-ce docker-ce-cli containerd.io

# Verify Docker installation
RUN docker --version

COPY target/sqrl-cli.jar /usr/src/app/sqrl-cli.jar
COPY dockerrun.sh /usr/src/app/dockerrun.sh
ENTRYPOINT ["/usr/src/app/dockerrun.sh"]
