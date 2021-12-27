package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.processor.SubscriptionProcessor;

public interface SubscriptionProcessorProvider {
  SubscriptionProcessor createSubscriptionProcessor();
}
