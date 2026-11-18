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

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

public class DuckDbExtensionIT {

  private static final String WAREHOUSE_DIR = "/iceberg-warehouse";

  @RegisterExtension static SqrlContainerExtension sqrl = new SqrlContainerExtension("duckdb");

  @Test
  @SneakyThrows
  void givenUdfScript_whenCompiledAndServerStarted_thenApiRespondsCorrectly() {
    var res = sqrl.sqrlCmd("test", "package.json");

    // Verify the expected success messages are present in the logs
    assertThat(res.logs())
        .contains("Running Tests")
        .contains("iceberg-query")
        .contains("postgres-query")
        .contains("BUILD SUCCESS");
  }

  @Test
  @SneakyThrows
  void givenLocalIcebergWarehouse_whenTestCommandExecutedTwice_thenRunsDoNotSeeEarlierRows() {
    var snapshots = sqrl.getTestDir().resolve("snapshots-tmp");
    FileUtils.deleteDirectory(snapshots.toFile());
    // Like a local warehouse directory, the volume keeps the tables of one invocation for the next
    var warehouse = createSessionVolume();
    Consumer<GenericContainer<?>> mountWarehouse =
        container ->
            container
                .withEnv("ICEBERG_TEST_WAREHOUSE", WAREHOUSE_DIR)
                .withCreateContainerCmdModifier(cmd -> mountVolume(cmd, warehouse));

    try {
      // Creates the snapshots, then verifies them with a second pipeline run on reset tables
      var created = sqrl.sqrlCmd(mountWarehouse, true, "test", "package-no-snapshots.json");
      assertThat(created.logs())
          .contains("Starting stream processor (snapshot validation run)")
          .contains("BUILD SUCCESS");
      assertThat(snapshots.resolve("iceberg-query.snapshot")).isRegularFile();

      // Compares with the snapshots while the warehouse still has the tables of the first one
      var compared = sqrl.sqrlCmd(mountWarehouse, true, "test", "package-no-snapshots.json");
      assertThat(compared.logs())
          .doesNotContain("Validating newly created snapshots")
          .contains("BUILD SUCCESS");
    } finally {
      FileUtils.deleteDirectory(snapshots.toFile());
    }
  }

  /** Creates a Docker volume that Testcontainers removes at the end of the test session. */
  private static String createSessionVolume() {
    var labels = new HashMap<>(DockerClientFactory.DEFAULT_LABELS);
    labels.put(DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL, DockerClientFactory.SESSION_ID);
    return DockerClientFactory.lazyClient().createVolumeCmd().withLabels(labels).exec().getName();
  }

  private static void mountVolume(CreateContainerCmd cmd, String volume) {
    var hostConfig = cmd.getHostConfig();
    var binds = new ArrayList<Bind>();
    if (hostConfig.getBinds() != null) {
      binds.addAll(List.of(hostConfig.getBinds()));
    }
    binds.add(new Bind(volume, new Volume(WAREHOUSE_DIR)));
    hostConfig.withBinds(binds);
  }
}
