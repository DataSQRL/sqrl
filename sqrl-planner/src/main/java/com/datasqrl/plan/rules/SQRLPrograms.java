/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import org.apache.calcite.tools.Program;

public class SQRLPrograms {

  public static Program ENUMERABLE_VOLCANO =
      (planner, rel, requiredOutputTraits, materializations, lattices) -> {
        if (rel.getTraitSet().equals(requiredOutputTraits)) {
          return rel;
        }

        var rel2 = planner.changeTraits(rel, requiredOutputTraits);
        planner.setRoot(rel2);

        final var planner2 = planner.chooseDelegate();
        final var rootRel3 = planner2.findBestExp();
        assert rootRel3 != null : "could not implement exp";
        return rootRel3;
      };

}
