package ai.dataeng.sqml.importer;

import ai.dataeng.sqml.execution.flink.ingest.DatasetLookup;
import lombok.Value;

@Value
public class DatasetImportManagerFactory implements ImportManagerFactory {
  DatasetLookup datasetLookup;

  public ImportManager3 create() {
    return new ImportManager3(datasetLookup);
  }
}
