package ai.datasqrl.config.provider;

import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.validate.imports.ImportManager;

public interface ImportManagerProvider {
  ImportManager createImportManager(DatasetRegistry datasetLookup);
}
