package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.processor.ImportProcessor;
import ai.dataeng.sqml.planner.operator.ImportResolver;

public interface ImportProcessorProvider {
  ImportProcessor createImportProcessor(ImportResolver importResolver,
      HeuristicPlannerProvider provider);
}
