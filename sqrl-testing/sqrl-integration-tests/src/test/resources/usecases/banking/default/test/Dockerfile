# Use a base image with Java (JMeter is a Java application)
FROM openjdk:11-jre

# Set the JMeter version
ENV JMETER_VERSION 5.6.3

# Install JMeter
RUN wget https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz -P /tmp/ \
    && tar -xzf /tmp/apache-jmeter-${JMETER_VERSION}.tgz -C /opt/ \
    && rm /tmp/apache-jmeter-${JMETER_VERSION}.tgz

# Set JMeter Home
ENV JMETER_HOME /opt/apache-jmeter-${JMETER_VERSION}

# Add JMeter to the PATH
ENV PATH $JMETER_HOME/bin:$PATH

# Copy the test plan into the container
# This line is optional here since you're mounting the test plan at runtime in the docker-compose file
# COPY ./test/test-plan.xml /test-plan.xml

# Set the working directory
WORKDIR /test

# Default command; can be overridden by docker-compose
CMD ["jmeter", "-n", "-t", "/test/test-plan.jmx", "-l", "/test/results.jtl"]