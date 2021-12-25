package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.SubscriptionType;

public interface SubscriptionProcessor {

  public void process(CreateSubscription statement, Namespace namespace);
}
