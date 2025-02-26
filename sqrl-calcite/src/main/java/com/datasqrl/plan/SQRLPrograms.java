/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;

public class SQRLPrograms {

  public static Program ENUMERABLE_VOLCANO =
      (planner, rel, requiredOutputTraits, materializations, lattices) -> {
        if (rel.getTraitSet().equals(requiredOutputTraits)) {
          return rel;
        }

        RelNode rel2 = planner.changeTraits(rel, requiredOutputTraits);
        planner.setRoot(rel2);

        final RelOptPlanner planner2 = planner.chooseDelegate();
        final RelNode rootRel3 = planner2.findBestExp();
        assert rootRel3 != null : "could not implement exp";
        return rootRel3;
      };
}
