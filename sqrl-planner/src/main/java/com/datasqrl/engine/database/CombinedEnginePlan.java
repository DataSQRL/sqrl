package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class CombinedEnginePlan implements EnginePhysicalPlan {

  @Singular
  Map<String, EnginePhysicalPlan> plans;

}
