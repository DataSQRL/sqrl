/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.plan.global.QueryIndexSummary.IndexableFunctionCall;
import com.datasqrl.v2.analyzer.TableAnalysis;

public class SqrlRelMdSelectivity extends RelMdSelectivity
    implements BuiltInMetadata.Selectivity.Handler {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, new SqrlRelMdSelectivity());

  @Override
  public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
    return super.getSelectivity(rel, mq, predicate);
  }


  public static Double getSelectivity(TableAnalysis table,
                                      QueryIndexSummary constraints) {
    //TODO: use actual selectivity statistics from table
    var selectivity = 1.0d;
    selectivity *= Math.pow(0.05,constraints.getEqualityColumns().size());
    selectivity *= Math.pow(0.5,constraints.getInequalityColumns().size());
    for (IndexableFunctionCall fcall : constraints.getFunctionCalls()) {
      selectivity *= fcall.getFunction().estimateSelectivity();
    }
    return selectivity;
  }

}