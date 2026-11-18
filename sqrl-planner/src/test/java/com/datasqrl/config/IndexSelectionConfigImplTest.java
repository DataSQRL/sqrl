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
package com.datasqrl.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexSelectionConfigImplTest {

  @TempDir private Path tempDir;

  private final ErrorCollector errors = ErrorCollector.root();

  @Test
  void givenDefaultPackage_whenGetIndexSelection_thenReturnsDefaults() {
    var indexSelection = loadIndexSelection(List.of());

    assertThat(indexSelection.getMaxIndexes()).isEqualTo(8);
    assertThat(indexSelection.getMaxIndexColumns()).isEqualTo(3);
    assertThat(indexSelection.getMaxFilterPatterns()).isEqualTo(50);
    assertThat(indexSelection.getMinCostReduction()).isEqualTo(0.05);
    assertThat(indexSelection.getSmallTableRowLimit()).isEqualTo(100);
    assertThat(errors).isEmpty();
  }

  @Test
  void givenOneKeyInPackage_whenGetIndexSelection_thenOtherKeysKeepDefaults() {
    var indexSelection =
        loadIndexSelection(List.of(writePackage("{\"small-table-row-limit\": 0}")));

    assertThat(indexSelection.getSmallTableRowLimit()).isZero();
    assertThat(indexSelection.getMaxIndexes()).isEqualTo(8);
    assertThat(indexSelection.getMinCostReduction()).isEqualTo(0.05);
    assertThat(errors).isEmpty();
  }

  @Test
  void givenCostReductionOfOne_whenLoadPackage_thenSchemaRejectsIt() {
    var packageFile = writePackage("{\"min-cost-reduction\": 1}");

    assertThatThrownBy(() -> ConfigLoaderUtils.loadUnresolvedConfig(errors, List.of(packageFile)))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("min-cost-reduction");
  }

  @Test
  void givenUnknownKey_whenLoadPackage_thenSchemaRejectsIt() {
    var packageFile = writePackage("{\"max-index\": 3}");

    assertThatThrownBy(() -> ConfigLoaderUtils.loadUnresolvedConfig(errors, List.of(packageFile)))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("max-index");
  }

  @Test
  void givenZeroMaxIndexes_whenGetMaxIndexes_thenThrowsError() {
    var config = SqrlConfig.createCurrentVersion();
    config.setProperty(IndexSelectionConfigImpl.MAX_INDEXES_KEY, 0);

    var indexSelection = new IndexSelectionConfigImpl(config);

    assertThatThrownBy(indexSelection::getMaxIndexes)
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Must be at least 1.");
  }

  @Test
  void givenNegativeCostReduction_whenGetMinCostReduction_thenThrowsError() {
    var config = SqrlConfig.createCurrentVersion();
    config.setProperty(IndexSelectionConfigImpl.MIN_COST_REDUCTION_KEY, -0.1);

    var indexSelection = new IndexSelectionConfigImpl(config);

    assertThatThrownBy(indexSelection::getMinCostReduction)
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Must be at least 0 and less than 1.");
  }

  private PackageJson.IndexSelectionConfig loadIndexSelection(List<Path> packageFiles) {
    return ConfigLoaderUtils.loadUnresolvedConfig(errors, packageFiles)
        .getEngines()
        .getEngineConfigOrEmpty("postgres")
        .getIndexSelection();
  }

  @SneakyThrows
  private Path writePackage(String indexSelection) {
    var packageFile = tempDir.resolve("package.json");
    Files.writeString(
        packageFile,
        "{\"engines\": {\"postgres\": {\"index-selection\": " + indexSelection + "}}}");
    return packageFile;
  }
}
