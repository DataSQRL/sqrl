package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.server.GenericJavaServerEngine.SqlQuery;
import com.datasqrl.graphql.SqrlObjectMapper;
import com.datasqrl.graphql.server.RootGraphqlModel;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

@AllArgsConstructor
@Getter
public class ServerPhysicalPlan implements EnginePhysicalPlan {

  @Setter
  RootGraphqlModel model;
  List<SqlQuery> queryList;
}
