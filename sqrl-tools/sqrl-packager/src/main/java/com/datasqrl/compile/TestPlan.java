package com.datasqrl.compile;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class TestPlan {

  List<GraphqlQuery> queries;
  List<GraphqlQuery> mutations;

  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  public static class GraphqlQuery {
    String name;
    String query;
  }
}
