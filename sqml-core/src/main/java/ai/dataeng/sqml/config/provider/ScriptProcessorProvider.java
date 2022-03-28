package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.ScriptProcessor;
import ai.dataeng.sqml.parser.operator.ImportResolver;
import ai.dataeng.sqml.schema.Namespace;

public interface ScriptProcessorProvider {
  ScriptProcessor createScriptProcessor(
      ImportResolver importResolver,
      HeuristicPlannerProvider plannerProvider,
      Namespace namespace);
}
