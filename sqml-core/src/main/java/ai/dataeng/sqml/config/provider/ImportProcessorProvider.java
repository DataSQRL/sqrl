package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.importer.DatasetManager;
import ai.dataeng.sqml.parser.processor.ImportProcessor;

public interface ImportProcessorProvider {
  ImportProcessor createImportProcessor(DatasetManager datasetManager);
}
