package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.importer.DatasetManager;

public interface ImportManagerProvider {
  DatasetManager createImportManager();
}
