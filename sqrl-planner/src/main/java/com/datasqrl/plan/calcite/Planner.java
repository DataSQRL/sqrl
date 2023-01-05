/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

/**
 * Process flow:
 * <p>
 * Convert Transpiled sqrl to SqlNode Validate SqlNode Convert to rel Execute planner rules via
 * 'transform' method (Planner rules ordered and indexed in Rules class)
 */
public class Planner extends SqrlPlannerImpl {

  public Planner(FrameworkConfig config) {
    super(config);
    ready();
  }

  public void refresh() {
    close();
    reset();
    ready();
  }

  public void setValidator(SqlNode sqlNode, SqlValidator validator) {
    state = State.STATE_4_VALIDATED;
    this.validatedSqlNode = sqlNode;
    this.validator = validator;
  }

  public RelBuilder getRelBuilder() {
    return sqlToRelConverterConfig.getRelBuilderFactory()
        .create(this.getCluster(), createCatalogReader());
  }

  public SqrlTypeRelDataTypeConverter getTypeConverter() {
    return new SqrlTypeRelDataTypeConverter(typeFactory);
  }

  public RelNode transform(OptimizationStage stage, RelNode node) {
    RelTraitSet outputTraits = getEmptyTraitSet();
    outputTraits = stage.getTrait().map(outputTraits::replace).orElse(outputTraits);
    return super.transform(stage.getIndex(), outputTraits, node);
  }
}