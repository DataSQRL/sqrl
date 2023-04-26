package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import lombok.Value;

@Value
public class ServerPhysicalPlan implements EnginePhysicalPlan {
  RootGraphqlModel model;
}
