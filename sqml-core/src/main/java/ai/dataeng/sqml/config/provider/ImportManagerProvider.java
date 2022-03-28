package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.parser.operator.ImportResolver;

public interface ImportManagerProvider {
  ImportResolver createImportManager(DatasetRegistry datasetLookup);
}
