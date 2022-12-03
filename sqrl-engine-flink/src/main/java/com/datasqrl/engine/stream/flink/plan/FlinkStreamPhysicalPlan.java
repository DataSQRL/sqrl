package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.engine.stream.StreamPhysicalPlan;
import lombok.Value;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

@Value
public class FlinkStreamPhysicalPlan implements StreamPhysicalPlan {

  StreamStatementSet statementSet;

}
