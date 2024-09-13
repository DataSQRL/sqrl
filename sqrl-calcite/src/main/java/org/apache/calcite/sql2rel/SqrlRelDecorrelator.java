package org.apache.calcite.sql2rel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public class SqrlRelDecorrelator extends RelDecorrelator {

  private final RelBuilder relBuilder;

  protected SqrlRelDecorrelator(CorelMap cm, Context context,
      RelBuilder relBuilder) {
    super(cm, context, relBuilder);
    this.relBuilder = relBuilder;
  }

  public static RelNode decorrelateQuery(RelNode rootRel,
      RelBuilder relBuilder) {
    final CorelMap corelMap = new CorelMapBuilder().build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final RelDecorrelator decorrelator =
        new SqrlRelDecorrelator(corelMap,
            cluster.getPlanner().getContext(), relBuilder);

    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);

    if (!decorrelator.cm.getMapCorToCorRel().isEmpty()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    //Sqrl: re-propagate the hints not necessary

    return newRootRel;
  }


//  public Frame decorrelateRel(Correlate rel) {
//    Frame frame = super.decorrelateRel(rel);
//    if (frame == null) {
//      return null;
//    }
//    RelNode newRel = relBuilder.push(frame.r)
//        .hints(rel.getHints())
//        .build();
//
//    return register(rel, newRel, frame.oldToNewOutputs, frame.corDefOutputs);
//  }
}
