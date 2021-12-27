package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.parser.processor.ScriptProcessor;
import ai.dataeng.sqml.parser.processor.DistinctProcessor;
import ai.dataeng.sqml.parser.processor.ExpressionProcessor;
import ai.dataeng.sqml.parser.processor.ImportProcessor;
import ai.dataeng.sqml.parser.processor.JoinProcessor;
import ai.dataeng.sqml.parser.processor.QueryProcessor;
import ai.dataeng.sqml.parser.processor.SubscriptionProcessor;

public interface ScriptProcessorProvider {
  ScriptProcessor createScriptProcessor(
      ImportProcessor importProcessor,
      QueryProcessor queryProcessor,
      ExpressionProcessor expressionProcessor,
      JoinProcessor joinProcessor,
      DistinctProcessor distinctProcessor,
      SubscriptionProcessor subscriptionProcessor,
      Namespace namespace);
}
