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
package com.datasqrl.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

public class IcebergTestExtension extends DuckdbTestExtension {

  @Override
  public void setup() {
    super.setup();
  }

  @Override
  public void teardown() {
    // assert that there is a 'my-table' iceberg table
    assertThat(
            Files.exists(
                Path.of("/tmp/duckdb/default_database/my-table/metadata/v1.metadata.json")))
        .isTrue();
    super.teardown();
  }
}
