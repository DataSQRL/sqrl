package ai.datasqrl.plan.global;

import java.util.EnumSet;

public interface IndexSelectorConfig {

    double getCostImprovementThreshold();

    EnumSet<IndexDefinition.Type> supportedIndexTypes();

    int maxIndexColumns(IndexDefinition.Type indexType);

    double relativeIndexCost(IndexDefinition index);

}
