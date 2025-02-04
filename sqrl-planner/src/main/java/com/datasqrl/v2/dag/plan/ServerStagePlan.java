package com.datasqrl.v2.dag.plan;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.v2.tables.SqrlTableFunction;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class ServerStagePlan implements EnginePhysicalPlan {

  /**
   * All accessible functions
   */
  @Singular
  List<SqrlTableFunction> functions;

  /**
   * All mutations
   */
  @Singular
  List<MutationQuery> mutations;

}
