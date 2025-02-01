package com.datasqrl.flinkwrapper.dag.plan;

import com.datasqrl.flinkwrapper.tables.SqrlTableFunction;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class ServerStagePlan {

  @Singular
  List<SqrlTableFunction> functions;

}
