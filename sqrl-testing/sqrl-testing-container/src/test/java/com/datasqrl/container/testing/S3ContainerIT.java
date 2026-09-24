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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class S3ContainerIT {

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("seedshop-tutorial");

  private static final String BUCKET_NAME = "test";
  private static final String CRED = "s3mock";
  private static final int S3_MOCK_PORT = 9090;

  private final GenericContainer<?> s3MockContainer =
      new GenericContainer<>(DockerImageName.parse("adobe/s3mock:5.2.2"))
          .withNetwork(sqrl.getNetwork())
          .withNetworkAliases("s3mock")
          .withExposedPorts(S3_MOCK_PORT)
          .waitingFor(
              Wait.forHttp("/favicon.ico")
                  .forPort(S3_MOCK_PORT)
                  .forStatusCode(200)
                  .withStartupTimeout(Duration.ofSeconds(30)));

  private AmazonS3 s3Client;

  @BeforeEach
  void setup() {
    s3MockContainer.start();

    var endpoint =
        "http://" + s3MockContainer.getHost() + ":" + s3MockContainer.getMappedPort(S3_MOCK_PORT);

    s3Client =
        AmazonS3Client.builder()
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(CRED, CRED)))
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(endpoint, "unused-region"))
            .build();

    s3Client.createBucket(BUCKET_NAME);
  }

  @AfterEach
  void tearDown() {
    sqrl.cleanupContainers();

    if (s3Client != null) {
      s3Client.shutdown();
    }

    if (s3MockContainer != null) {
      s3MockContainer.stop();
    }
  }

  @Test
  void test() {
    var cmd =
        sqrl.createCmdContainer()
            .withNetwork(sqrl.getNetwork())
            .withEnv("AWS_ACCESS_KEY_ID", CRED)
            .withEnv("AWS_SECRET_KEY", CRED)
            .withCommand("test", "package-s3.json");
    cmd.start();

    await().atMost(Duration.ofSeconds(90)).until(() -> !cmd.isRunning());
    assertThat(cmd.getLogs()).contains("BUILD SUCCESS");

    var objList = s3Client.listObjects(BUCKET_NAME);
    var objSummaries = objList.getObjectSummaries();
    assertThat(objSummaries).hasSize(1);

    var objKey = objSummaries.get(0).getKey();
    var data = s3Client.getObjectAsString(BUCKET_NAME, objKey);

    assertThat(data).hasLineCount(15);
  }
}
