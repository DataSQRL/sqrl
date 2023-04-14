package com.datasqrl.plan;

import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;

public class SqrlRelBuilder extends RelBuilder {

  public SqrlRelBuilder(RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    super(null, cluster, relOptSchema);
  }

  public static SqrlRelBuilder create(RelOptCluster cluster, SqrlSchema schema) {
    return new SqrlRelBuilder(cluster, SqrlPlannerConfigFactory.createCatalogReader(schema));
  }
}
