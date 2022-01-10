package org.apache.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;

public class SqrlRelBuilder extends RelBuilder {

	public SqrlRelBuilder(org.apache.calcite.plan.Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
		super(context, cluster, relOptSchema);
	}

}