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

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Runs a pipeline whose Iceberg warehouse is on S3Mock. Flink writes the table through the Hadoop
 * catalog, and the server reads it through DuckDB with the {@code use-credential-chain} secret and
 * {@code s3-endpoint}, so the init SQL of those two keys runs against the extensions in the image.
 */
public class DuckDbS3ContainerIT {

  @RegisterExtension static SqrlContainerExtension sqrl = new SqrlContainerExtension("duckdb-s3");

  private static final String BUCKET_NAME = "warehouse";

  @RegisterExtension
  static S3MockContainerExtension s3Mock = new S3MockContainerExtension(sqrl, BUCKET_NAME);

  /** Its core-site.xml sends the Iceberg writes of Flink to S3Mock. */
  private static final String HADOOP_CONF_DIR =
      SqrlContainerExtension.WORKSPACE_DIR + "/hadoop-conf";

  @Test
  void givenIcebergWarehouseOnS3_whenTestRuns_thenDuckDbReadsItWithCredentialChainSecret() {
    var res =
        sqrl.sqrlCmd(
            cmd ->
                cmd.withNetwork(sqrl.getNetwork())
                    .withEnv("AWS_ACCESS_KEY_ID", S3MockContainerExtension.CREDENTIAL)
                    .withEnv("AWS_SECRET_ACCESS_KEY", S3MockContainerExtension.CREDENTIAL)
                    .withEnv("AWS_REGION", "us-east-1")
                    .withEnv("HADOOP_CONF_DIR", HADOOP_CONF_DIR),
            true,
            "test",
            "package.json");

    assertThat(res.logs()).contains("ApplicationsTest").contains("BUILD SUCCESS");

    // The table exists only in the bucket, so the snapshot matched on rows DuckDB read from S3Mock
    assertThat(s3Mock.getClient().listObjectsV2(BUCKET_NAME, "iceberg/").getObjectSummaries())
        .extracting(S3ObjectSummary::getKey)
        .anyMatch(key -> key.endsWith(".parquet"))
        .anyMatch(key -> key.endsWith(".metadata.json"));
  }
}
