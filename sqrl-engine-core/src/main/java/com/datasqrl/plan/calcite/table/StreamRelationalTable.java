/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.schema.UniversalTableBuilder;
import org.apache.calcite.rel.RelNode;

public interface StreamRelationalTable extends SourceRelationalTable {

  UniversalTableBuilder getStreamSchema();

  RelNode getBaseRelation();

  StateChangeType getStateChangeType();

}
