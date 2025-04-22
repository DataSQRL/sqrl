package com.datasqrl.engine.server;

import java.util.List;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.v2.dag.plan.MutationQuery;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
public class ServerPhysicalPlan implements EnginePhysicalPlan {

  /**
   * The endpoint functions for the server plan
   */
  @JsonIgnore
  List<SqrlTableFunction> functions;
  /**
   * The mutation endpoints
   */
  @JsonIgnore
  List<MutationQuery> mutations;
  /**
   * The generated API for the server. This gets generated after the
   * planning and is added to the plan later.
   *
   * TODO: generalize to support multiple types of APIs (e.g. REST, GraphQL, gRPC) and make a list
   */
  @Setter
  RootGraphqlModel model;
}
