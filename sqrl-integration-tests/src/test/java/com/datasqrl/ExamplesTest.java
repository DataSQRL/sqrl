package com.datasqrl;

import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.util.TestScript;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class ExamplesTest extends AbstractPhysicalSQRLIT {

  @Disabled
  @ParameterizedTest
  @ArgumentsSource(TestScript.ExampleScriptsProvider.class)
  public void test(TestScript script) {
    initialize(IntegrationTestSettings.getFlinkWithDB(DatabaseEngine.H2),
        script.getRootPackageDirectory());
    validateTables(script.getScript(), script.getResultTables()
        .toArray(new String[0]));

    System.out.println(script);
  }
}
