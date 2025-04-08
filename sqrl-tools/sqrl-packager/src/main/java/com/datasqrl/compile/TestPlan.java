package com.datasqrl.compile;

import java.util.List;

import com.datasqrl.compile.TestPlan.GraphqlQuery;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class TestPlan {

  List<GraphqlQuery> queries;
  List<GraphqlQuery> mutations;
  List<GraphqlQuery> subscriptions;

  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  public static class GraphqlQuery {
    String name;
    String query;
  }
}
