package ai.datasqrl.config.provider;

import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.server.ImportManager;

public interface ImportManagerProvider {

  ImportManager createImportManager(DatasetRegistry datasetLookup);
}
