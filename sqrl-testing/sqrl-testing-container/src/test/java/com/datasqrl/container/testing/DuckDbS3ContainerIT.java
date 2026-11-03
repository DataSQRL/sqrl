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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs a pipeline whose Iceberg warehouse is on S3Mock. Flink writes the table through the Hadoop
 * catalog, and the server reads it through DuckDB with the {@code use-credential-chain} secret and
 * {@code s3-endpoint}, so the init SQL of those two keys runs against the extensions in the image.
 */
public class DuckDbS3ContainerIT {

  @RegisterExtension static SqrlContainerExtension sqrl = new SqrlContainerExtension("duckdb-s3");

  private static final String BUCKET_NAME = "warehouse";
  private static final String CRED = "s3mock";
  private static final int S3_MOCK_PORT = 9090;

  /** Its core-site.xml sends the Iceberg writes of Flink to S3Mock. */
  private static final String HADOOP_CONF_DIR =
      SqrlContainerExtension.WORKSPACE_DIR + "/hadoop-conf";

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
    if (s3Client != null) {
      s3Client.shutdown();
    }

    s3MockContainer.stop();
  }

  @Test
  void givenIcebergWarehouseOnS3_whenTestRuns_thenDuckDbReadsItWithCredentialChainSecret() {
    var res =
        sqrl.sqrlCmd(
            cmd ->
                cmd.withNetwork(sqrl.getNetwork())
                    .withEnv("AWS_ACCESS_KEY_ID", CRED)
                    .withEnv("AWS_SECRET_ACCESS_KEY", CRED)
                    .withEnv("AWS_REGION", "us-east-1")
                    .withEnv("HADOOP_CONF_DIR", HADOOP_CONF_DIR),
            true,
            "test",
            "package.json");

    assertThat(res.logs()).contains("ApplicationsTest").contains("BUILD SUCCESS");

    // The table exists only in the bucket, so the snapshot matched on rows DuckDB read from S3Mock
    assertThat(s3Client.listObjectsV2(BUCKET_NAME, "iceberg/").getObjectSummaries())
        .extracting(S3ObjectSummary::getKey)
        .anyMatch(key -> key.endsWith(".parquet"))
        .anyMatch(key -> key.endsWith(".metadata.json"));
  }
}
