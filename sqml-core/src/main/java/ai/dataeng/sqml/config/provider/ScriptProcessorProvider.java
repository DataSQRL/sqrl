package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.parser.processor.ScriptProcessor;
import ai.dataeng.sqml.planner.operator.ImportResolver;

public interface ScriptProcessorProvider {
  ScriptProcessor createScriptProcessor(
      ImportResolver importResolver,
      HeuristicPlannerProvider plannerProvider,
      Namespace namespace);
}
