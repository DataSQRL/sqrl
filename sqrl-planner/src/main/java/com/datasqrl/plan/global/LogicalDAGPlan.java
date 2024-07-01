package com.datasqrl.plan.global;

import com.datasqrl.engine.log.Log;
import java.util.List;
import lombok.Value;

@Value
public class LogicalDAGPlan {

  SqrlDAG dag;
  List<Log> logs;

}
