package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.server.RootGraphqlModel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
public class ServerPhysicalPlan implements EnginePhysicalPlan {

  @Setter RootGraphqlModel model;
}
