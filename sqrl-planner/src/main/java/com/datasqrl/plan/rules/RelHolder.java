/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import org.apache.calcite.rel.RelNode;

public interface RelHolder {

  RelNode getRelNode();
}
