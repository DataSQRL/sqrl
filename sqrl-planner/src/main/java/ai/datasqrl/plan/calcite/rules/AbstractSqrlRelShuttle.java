package com.datasqrl.plan.calcite.rules;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;

public abstract class AbstractSqrlRelShuttle<V extends RelHolder> implements SqrlRelShuttle {

    protected V relHolder = null;

    protected RelNode setRelHolder(V relHolder) {
        this.relHolder = relHolder;
        return relHolder.getRelNode();
    }

    protected V getRelHolder(RelNode node) {
        Preconditions.checkArgument(this.relHolder.getRelNode().equals(node));
        V relHolder = this.relHolder;
        this.relHolder = null;
        return relHolder;
    }



}
