/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import org.apache.calcite.rel.RelNode;

import com.google.common.base.Preconditions;

public abstract class AbstractSqrlRelShuttle<V extends RelHolder> implements SqrlRelShuttle {

  protected V relHolder = null;

  protected RelNode setRelHolder(V relHolder) {
    this.relHolder = relHolder;
    return relHolder.getRelNode();
  }

  protected V getRelHolder(RelNode node) {
    Preconditions.checkArgument(this.relHolder.getRelNode().equals(node));
    var relHolder = this.relHolder;
    this.relHolder = null;
    return relHolder;
  }


}
