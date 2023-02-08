package com.datasqrl.plan.calcite;

import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;

public class SqrlRelBuilder extends RelBuilder {

  public SqrlRelBuilder(RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    super(null, cluster, relOptSchema);
  }

  public static SqrlRelBuilder create(RelOptCluster cluster, SqrlCalciteSchema schema) {
    return new SqrlRelBuilder(cluster, SqrlPlannerConfigFactory.createCatalogReader(schema));
  }
}
