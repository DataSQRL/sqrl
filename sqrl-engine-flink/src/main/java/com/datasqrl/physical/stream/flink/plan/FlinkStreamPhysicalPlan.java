package com.datasqrl.physical.stream.flink.plan;

import com.datasqrl.physical.stream.StreamPhysicalPlan;
import lombok.Value;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

@Value
public class FlinkStreamPhysicalPlan implements StreamPhysicalPlan {

  StreamStatementSet statementSet;

}
