/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite;

import org.apache.calcite.rel.RelNode;

public class RelPrinter {

  public static String explain(RelNode relNode) {
    return relNode.explain();
  }
}
