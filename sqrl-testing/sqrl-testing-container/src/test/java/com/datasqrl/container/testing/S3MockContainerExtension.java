/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.container.testing;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.time.Duration;
import java.util.Objects;
import lombok.Getter;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Starts S3Mock on the network of a {@link SqrlContainerExtension} for each test and creates one
 * bucket in it. Other containers reach it at {@code http://s3mock:9090}, and the test reads the
 * bucket through {@link #getClient()}. It needs only the network that {@link
 * SqrlContainerExtension} creates before all tests, so the order of the two extensions does not
 * matter.
 */
public class S3MockContainerExtension implements BeforeEachCallback, AfterEachCallback {

  private static final String NETWORK_ALIAS = "s3mock";
  private static final int PORT = 9090;

  /** Access key id and secret access key for clients. S3Mock does not check them. */
  public static final String CREDENTIAL = "s3mock";

  private static final String IMAGE = "adobe/s3mock:5.2.2";

  private final SqrlContainerExtension sqrl;
  private final String bucketName;

  private GenericContainer<?> s3Mock;

  /** Client for the running S3Mock, through its port on the host. */
  @Getter private AmazonS3 client;

  public S3MockContainerExtension(SqrlContainerExtension sqrl, String bucketName) {
    this.sqrl = Objects.requireNonNull(sqrl);
    this.bucketName = Objects.requireNonNull(bucketName);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    s3Mock =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withNetwork(
                Objects.requireNonNull(
                    sqrl.getNetwork(), "SqrlContainerExtension must be registered on the class"))
            .withNetworkAliases(NETWORK_ALIAS)
            .withExposedPorts(PORT)
            .waitingFor(
                Wait.forHttp("/favicon.ico")
                    .forPort(PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(30)));
    s3Mock.start();

    var endpoint = "http://" + s3Mock.getHost() + ":" + s3Mock.getMappedPort(PORT);

    client =
        AmazonS3Client.builder()
            .withCredentials(
                new AWSStaticCredentialsProvider(new BasicAWSCredentials(CREDENTIAL, CREDENTIAL)))
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(endpoint, "unused-region"))
            .build();

    client.createBucket(bucketName);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    if (client != null) {
      client.shutdown();
      client = null;
    }

    if (s3Mock != null) {
      s3Mock.stop();
      s3Mock = null;
    }
  }
}
