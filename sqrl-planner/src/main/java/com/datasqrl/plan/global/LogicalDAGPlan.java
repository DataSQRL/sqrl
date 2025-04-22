package com.datasqrl.plan.global;

import java.util.List;

import com.datasqrl.engine.log.Log;

import lombok.Value;

@Value
public class LogicalDAGPlan {
  List<Log> logs;
}
