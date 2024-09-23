package com.datasqrl.config;

import static com.datasqrl.config.ConfigurationTest.CONFIG_DIR;
import static com.datasqrl.config.ConfigurationTest.testForErrors;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class PackageJsonSchemaTest {

  private static final Path TEST_CASES = Path.of(CONFIG_DIR.toString(), "package");

  @ParameterizedTest
  @ValueSource(strings = {
      "missingVersionField.json",
      "validIcebergConfig.json",
      "missingCompileSection.json",
      "validDependencies.json",
      "missingProfilesField.json",
      "validPackageWithUrls.json"
  })
  public void testValidConfigFile(String configFileName) {
    ErrorCollector errors = ErrorCollector.root();
    try {
      SqrlConfigCommons.fromFilesPackageJson(errors, List.of(TEST_CASES.resolve(configFileName)));
    } catch (Exception e) {
      fail("Unexpected error: " + errors.getErrors().toString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "emptyEnabledEngines.json",
      "invalidVersionFormat.json",
      "additionalPropertyInIceberg.json",
      "invalidConnectorInIceberg.json",
      "emptyEnginesFlinkConnectors.json",
      "missingRequiredDependencyFields.json",
      "invalidDurationInFlinkConfig.json",
      "emptyTestRunner.json",
      "invalidUrlInPackage.json"
  })
  public void testInvalidConfigFile(String configFileName) {
    testForErrors(errors -> SqrlConfigCommons.fromFilesPackageJson(errors,
        List.of(CONFIG_DIR.resolve(configFileName))));
  }
}
