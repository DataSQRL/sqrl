package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.processor.QueryProcessor;

public interface QueryProcessorProvider {
  QueryProcessor createQueryProcessor(HeuristicPlannerProvider plannerProvider);
}
