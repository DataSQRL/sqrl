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
package com.datasqrl;

import static com.datasqrl.SnapshotTestSupport.getResourcesDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class UnusedEndpointsWarningTest {

  private static final Path PROJECT_DIR = getResourcesDirectory("unused-endpoints");
  private static final String WARNING = "No GraphQL schema uses these endpoints";

  @RegisterExtension final CliCompileTestExtension compileExtension = new CliCompileTestExtension();

  @Test
  void givenCustomSchemaLeavingOutEndpoints_whenCompile_thenWarnsAboutNonIcebergEndpoints()
      throws IOException {
    var status = compileExtension.execute(PROJECT_DIR, List.of("compile", "package.json"));

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages())
        .contains(
            WARNING
                + ", but they are still computed and written to the database or log: EventAlert,"
                + " RecentEvents, SiteCount.events.");
    // EventArchive is left out of the schema too, but it is an Iceberg view
    assertThat(Files.readString(compileExtension.getPlanDir().resolve("iceberg-duckdb-views.sql")))
        .contains("EventArchive");
  }

  @Test
  void givenTwoApiVersions_whenCompile_thenWarnsOnlyAboutEndpointsThatNoVersionServes() {
    // v1 serves SiteCount and v2 serves RecentEvents
    var status = compileExtension.execute(PROJECT_DIR, List.of("compile", "package-versions.json"));

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages())
        .contains(
            WARNING
                + ", but they are still computed and written to the database or log:"
                + " EventAlert, SiteCount.events.");
  }

  @Test
  void givenCustomSchemaAndTestTable_whenTest_thenDoesNotWarnAboutTestTable() {
    var status = compileExtension.execute(PROJECT_DIR, List.of("test", "package-test-schema.json"));

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages())
        .contains(
            WARNING
                + ", but they are still computed and written to the database or log:"
                + " EventAlert, RecentEvents, SiteCount.events.");
  }

  @Test
  void givenInferredSchema_whenCompile_thenDoesNotWarn() {
    var status =
        compileExtension.execute(PROJECT_DIR, List.of("compile", "package-inferred-schema.json"));

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages()).doesNotContain(WARNING);
  }
}
