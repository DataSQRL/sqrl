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

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class S3ContainerIT {

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("seedshop-tutorial");

  private static final String BUCKET_NAME = "test";

  @RegisterExtension
  static S3MockContainerExtension s3Mock = new S3MockContainerExtension(sqrl, BUCKET_NAME);

  @AfterEach
  void tearDown() {
    // Stops the cmd container before the extension stops S3Mock
    sqrl.cleanupContainers();
  }

  @Test
  void test() {
    var cmd =
        sqrl.createCmdContainer()
            .withNetwork(sqrl.getNetwork())
            .withEnv("AWS_ACCESS_KEY_ID", S3MockContainerExtension.CREDENTIAL)
            .withEnv("AWS_SECRET_KEY", S3MockContainerExtension.CREDENTIAL)
            .withCommand("test", "package-s3.json");
    cmd.start();

    await().atMost(Duration.ofSeconds(90)).until(() -> !cmd.isRunning());
    assertThat(cmd.getLogs()).contains("BUILD SUCCESS");

    var s3Client = s3Mock.getClient();
    var objList = s3Client.listObjects(BUCKET_NAME);
    var objSummaries = objList.getObjectSummaries();
    assertThat(objSummaries).hasSize(1);

    var objKey = objSummaries.get(0).getKey();
    var data = s3Client.getObjectAsString(BUCKET_NAME, objKey);

    assertThat(data).hasLineCount(15);
  }
}
