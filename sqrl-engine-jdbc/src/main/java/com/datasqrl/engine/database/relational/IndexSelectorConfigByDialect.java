package com.datasqrl.engine.database.relational;

import com.datasqrl.config.provider.Dialect;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelectorConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

import java.util.EnumSet;

import static com.datasqrl.plan.global.IndexDefinition.Type.BTREE;
import static com.datasqrl.plan.global.IndexDefinition.Type.HASH;

@Value
@Builder
public class IndexSelectorConfigByDialect implements IndexSelectorConfig {

  public static final double DEFAULT_COST_THRESHOLD = 0.95;
  public static final int MAX_INDEX_COLUMNS = 6;

  @Getter
  @Builder.Default
  double costImprovementThreshold = DEFAULT_COST_THRESHOLD;
  @NonNull
  Dialect dialect;
  @Builder.Default
  int maxIndexColumns = MAX_INDEX_COLUMNS;

  @Override
  public EnumSet<IndexDefinition.Type> supportedIndexTypes() {
    switch (dialect) {
      case POSTGRES:
        return EnumSet.of(HASH, BTREE);
      case MYSQL:
        return EnumSet.of(HASH, BTREE);
      case H2:
        return EnumSet.of(HASH, BTREE);
      default:
        throw new IllegalStateException(dialect.name());
    }
  }

  @Override
  public int maxIndexColumns(IndexDefinition.Type indexType) {
    switch (dialect) {
      case POSTGRES:
        switch (indexType) {
          case HASH:
            return 1;
          default:
            return maxIndexColumns;
        }
      case MYSQL:
        return maxIndexColumns;
      case H2:
        return maxIndexColumns;
      default:
        throw new IllegalStateException(dialect.name());
    }
  }

  @Override
  public double relativeIndexCost(IndexDefinition index) {
    switch (index.getType()) {
      case HASH:
        return 1;
      case BTREE:
        return 1.5 + 0.1 * index.getColumns().size();
      default:
        throw new IllegalStateException(index.getType().name());
    }
  }

  public static IndexSelectorConfigByDialect of(Dialect dialect) {
    return IndexSelectorConfigByDialect.builder().dialect(dialect).build();
  }

}
