package ai.datasqrl;

import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.DataSystemDiscoveryConfig;
import ai.datasqrl.util.DatabaseHandle;
import ai.datasqrl.util.TestDataset;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractEngineIT {

  public DatabaseHandle database = null;
  public SqrlSettings sqrlSettings = null;


  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.cleanUp();
      database = null;
    }
  }

  protected void initialize(IntegrationTestSettings settings) {
    Pair<DatabaseHandle, SqrlSettings> setup = settings.getSqrlSettings();
    sqrlSettings = setup.getRight();
    database = setup.getLeft();
  }

  protected DataSystemConfig.DataSystemConfigBuilder getSystemConfigBuilder(TestDataset testDataset) {
    DataSystemDiscoveryConfig datasource = DirectoryDataSystemConfig.of(testDataset.getDataDirectory());
    DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
    builder.datasource(datasource);
    builder.name(testDataset.getName());
    return builder;
  }

}
