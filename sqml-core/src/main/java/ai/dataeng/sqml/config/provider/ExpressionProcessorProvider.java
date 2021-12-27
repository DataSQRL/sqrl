package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.processor.ExpressionProcessor;

public interface ExpressionProcessorProvider {
  ExpressionProcessor createExpressionProcessor(HeuristicPlannerProvider plannerProvider);
}
