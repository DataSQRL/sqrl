/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.schema.UniversalTable;
import java.io.Serializable;
import org.apache.calcite.rel.RelNode;

public interface StreamRelationalTable extends SourceRelationalTable {

  UniversalTable getStreamSchema();

  RelNode getBaseRelation();

  StateChangeType getStateChangeType();

  BaseTableMetaData getBaseTableMetaData();


  interface BaseTableMetaData extends Serializable {

    int[] getKeyIdx();

    int[] getSelectIdx();

    boolean hasTimestamp();

    int getTimestampIdx();

  }

}
