/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.model.LogicalStreamMetaData;
import lombok.NonNull;
import lombok.Value;
import com.datasqrl.model.StreamType;
import org.apache.flink.table.api.Table;

@Value
public class StreamTableConverterContext {

  @NonNull Table inputTable;
  @NonNull StreamType streamType;

  boolean unmodifiedChangelog;
  @NonNull
  LogicalStreamMetaData streamMetaData;


}
