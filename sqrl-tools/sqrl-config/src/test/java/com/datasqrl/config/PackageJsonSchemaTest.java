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

import static com.datasqrl.config.ConfigurationTest.CONFIG_DIR;
import static com.datasqrl.config.ConfigurationTest.testForErrors;
import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PackageJsonSchemaTest {

  private static final Path TEST_CASES = Path.of(CONFIG_DIR.toString(), "package");

  @ParameterizedTest
  @ValueSource(
      strings = {
        "missingVersionField.json",
        "validIcebergConfig.json",
        "missingCompileSection.json",
        "validDependencies.json",
        "missingProfilesField.json",
        "validPackageWithUrls.json",
        "onlyVersionFieldExists.json"
      })
  void validConfigFile(String configFileName) {
    var errors = ErrorCollector.root();
    try {
      SqrlConfigCommons.fromFilesPackageJson(errors, List.of(TEST_CASES.resolve(configFileName)));
    } catch (Exception e) {
      fail("Unexpected error: " + errors.getErrors().toString());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "emptyEnabledEngines.json",
        "invalidVersionFormat.json",
        "additionalPropertyInIceberg.json",
        "invalidConnectorInIceberg.json",
        "emptyEnginesFlinkConnectors.json",
        "missingRequiredDependencyFields.json",
        "invalidDurationInFlinkConfig.json",
        "emptyTestRunner.json",
        "invalidUrlInPackage.json",
        "emptyPropertiesInPackage.json"
      })
  void invalidConfigFile(String configFileName) {
    testForErrors(
        errors ->
            SqrlConfigCommons.fromFilesPackageJson(
                errors, List.of(CONFIG_DIR.resolve(configFileName))));
  }
}
