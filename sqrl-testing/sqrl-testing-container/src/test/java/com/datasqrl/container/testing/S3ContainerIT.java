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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;

public class S3ContainerIT extends SqrlContainerTestBase {

  private static final String BUCKET_NAME = "test";
  private static final String CRED = "minioadmin";

  private final MinIOContainer minioContainer =
      new MinIOContainer("minio/minio:RELEASE.2023-12-20T01-00-02Z")
          .withNetwork(sharedNetwork)
          .withNetworkAliases("minio")
          .withUserName(CRED)
          .withPassword(CRED)
          .withExposedPorts(9000, 9001);

  private AmazonS3 s3Client;

  @BeforeEach
  void setup() {
    minioContainer.start();

    // Get MinIO connection details
    String endpoint = minioContainer.getS3URL();
    String accessKey = minioContainer.getUserName();
    String secretKey = minioContainer.getPassword();

    s3Client =
        AmazonS3Client.builder()
            .withCredentials(
                new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(endpoint, "unused-region"))
            .build();

    s3Client.createBucket(BUCKET_NAME);
  }

  @Override
  protected void cleanupContainers() {
    super.cleanupContainers();

    if (s3Client != null) {
      s3Client.shutdown();
    }

    if (minioContainer != null) {
      minioContainer.stop();
    }
  }

  @Override
  protected String getTestCaseName() {
    return "seedshop-tutorial";
  }

  @Test
  void test() {
    cmd =
        createCmdContainer(testDir)
            .withNetwork(sharedNetwork)
            .withEnv("AWS_ACCESS_KEY_ID", CRED)
            .withEnv("AWS_SECRET_KEY", CRED)
            .withCommand("test", "-c", "package-s3.json");

    cmd.start();

    await().atMost(Duration.ofSeconds(90)).until(() -> !cmd.isRunning());
    assertThat(cmd.getLogs()).contains("Snapshot OK");

    var objList = s3Client.listObjects(BUCKET_NAME);
    var objSummaries = objList.getObjectSummaries();
    assertThat(objSummaries).hasSize(1);

    var objKey = objSummaries.get(0).getKey();
    var data = s3Client.getObjectAsString(BUCKET_NAME, objKey);

    assertThat(data).hasLineCount(15);
  }
}
