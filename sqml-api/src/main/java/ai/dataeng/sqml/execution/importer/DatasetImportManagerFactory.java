package ai.dataeng.sqml.execution.importer;

import ai.dataeng.sqml.ingest.DatasetLookup;
import lombok.Value;

@Value
public class DatasetImportManagerFactory implements ImportManagerFactory {
  DatasetLookup datasetLookup;

  public ImportManager create() {
    return new ImportManager(datasetLookup);
  }
}
