package ai.datasqrl.config.provider;

import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;

public interface ImportManagerProvider {

  ImportManager createImportManager(DatasetRegistry datasetLookup);
}
