package com.datasqrl.plan.local.analyze;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Disabled
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
  public void basicImportErrors() {
    initialize(IntegrationTestSettings.getInMemory(), (Path) null, Optional.empty());
    generateInvalid("import",
        "IMPORT ecommerce-data.Customerrr");
  }

  @Test
  public void basicExportErrors() {
    initialize(IntegrationTestSettings.getInMemory(), (Path) null, Optional.empty());
    generateInvalid("export",
        "IMPORT ecommerce-data.Customer",
        "EXPORT Customer TO outputt.customer");
  }

  @Test
  public void logicalPlanErrors() {
    initialize(IntegrationTestSettings.getInMemory(), (Path) null, Optional.empty());
    generateInvalid("distincton",
        "IMPORT ecommerce-data.Customer TIMESTAMP _ingest_time",
        "Customer1 := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC",
        "Customer2 := DISTINCT Customer1 ON customerid ORDER BY _ingest_time DESC");
  }

  @Test
  @Disabled("move to physical error handling test")
  public void debugErrors() {
    initialize(IntegrationTestSettings.builder()
        .build(), (Path) null, Optional.empty());
    generateInvalid("wrong-sink",
        "IMPORT ecommerce-data.Customer",
        "Customer2 := SELECT * FROM Customer WHERE customerid > 0",
        "EXPORT Customer2 TO print.customer2");
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
      snapshot.addContent(ErrorPrinter.prettyPrint(errors), caseName);
    }
  }
}
