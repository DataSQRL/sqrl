package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.parser.operator.ImportManager;

public interface ImportManagerProvider {
  ImportManager createImportManager(DatasetRegistry datasetLookup);
}
