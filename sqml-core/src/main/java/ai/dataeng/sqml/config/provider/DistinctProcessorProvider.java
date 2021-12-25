package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.processor.DistinctProcessor;

public interface DistinctProcessorProvider {
  DistinctProcessor createDistinctProcessor();
}
