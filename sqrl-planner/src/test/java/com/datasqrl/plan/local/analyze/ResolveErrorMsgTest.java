package com.datasqrl.plan.local.analyze;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class ResolveErrorMsgTest extends AbstractLogicalSQRLIT {

  private final Retail example = Retail.INSTANCE;
  private Path exportPath = example.getRootPackageDirectory().resolve("export-data");

  protected SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    if (!Files.isDirectory(exportPath)) {
      Files.createDirectory(exportPath);
    }

  }

  @AfterEach
  @SneakyThrows
  public void tearDown() {
    super.tearDown();
    snapshot.createOrValidate();
    if (Files.isDirectory(exportPath)) {
      FileUtils.deleteDirectory(exportPath.toFile());
    }
  }

  @Test
  public void basicErrors() {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
    generateInvalid("import",
        "IMPORT ecommerce-data.Customerrr");
    generateInvalid("export",
        "IMPORT ecommerce-data.Customer",
        "EXPORT Customer TO outputt.customer");
  }

  @Test
  public void debugErrors() {
    initialize(IntegrationTestSettings.builder().debugger(
            DebuggerConfig.of(NamePath.of("missing"),null))
        .build(), example.getRootPackageDirectory());
    generateInvalid("wrong-sink",
        "IMPORT ecommerce-data.Customer",
        "Customer2 := SELECT * FROM Customer WHERE customerid > 0");
  }

  private void generateInvalid(String caseName, String... lines) {
    String script = Arrays.stream(lines).map(l -> l + ";\n").reduce("",(s1,s2) -> s1+s2);
    generateInvalid(caseName, script);
  }

  private void generateInvalid(String caseName, String script) {
    try {
      plan(script);
      fail("Should throw exception");
    } catch (Exception e) {
      snapshot.addContent(ErrorPrinter.prettyPrint(error), caseName);
//      e.printStackTrace();
    }


  }




}
