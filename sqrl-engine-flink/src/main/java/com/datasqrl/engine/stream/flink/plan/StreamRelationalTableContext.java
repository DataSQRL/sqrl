package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.plan.calcite.table.StateChangeType;
import lombok.Value;
import org.apache.flink.table.api.Table;

@Value
public class StreamRelationalTableContext  {

  Table inputTable;
  StateChangeType stateChangeType;
}
