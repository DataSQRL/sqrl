package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.processor.JoinProcessor;

public interface JoinProcessorProvider {
  JoinProcessor createJoinProcessor();
}
