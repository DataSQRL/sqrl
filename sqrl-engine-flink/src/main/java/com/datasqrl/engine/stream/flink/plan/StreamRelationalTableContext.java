/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.plan.calcite.table.StateChangeType;
import com.datasqrl.plan.calcite.table.StreamRelationalTable.BaseTableMetaData;
import lombok.Value;
import org.apache.flink.table.api.Table;

@Value
public class StreamRelationalTableContext  {

  Table inputTable;
  StateChangeType stateChangeType;

  boolean unmodifiedChangelog;
  BaseTableMetaData baseTableMetaData;


}
