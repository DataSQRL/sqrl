package com.datasqrl.compile;

import java.util.List;
import lombok.Getter;

@Getter
public class TestPlan2  extends TestPlan {
  List<GraphqlQuery> subscriptions;

  public TestPlan2(List<GraphqlQuery> queries, List<GraphqlQuery> mutations, List<GraphqlQuery> subscriptions) {
    super(queries, mutations);
    this.subscriptions = subscriptions;
  }
}
