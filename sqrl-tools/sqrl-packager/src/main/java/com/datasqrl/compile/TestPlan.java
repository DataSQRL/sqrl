package com.datasqrl.compile;

import java.util.List;
import lombok.Value;

@Value
public class TestPlan {

  List<GraphqlQuery> queries;
  List<GraphqlQuery> mutations;

  @Value
  public static class GraphqlQuery {
    String name;
    String query;
  }
}
