package com.datasqrl.plan.global;

import com.datasqrl.plan.calcite.rules.SQRLConverter;
import com.datasqrl.plan.queries.APIQuery;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class AnalyzedAPIQuery {

  private APIQuery baseQuery;
  //TODO: add configuration options for runtime

  public AnalyzedAPIQuery(APIQuery baseQuery) {
    this.baseQuery = baseQuery;
  }

  public AnalyzedAPIQuery(String nameId, RelNode relNode) {
    this.baseQuery = new APIQuery(nameId, relNode);
  }

  public SQRLConverter.Config getBaseConfig() {
    return SQRLConverter.Config.builder()
        .setOriginalFieldnames(true).build();
  }

  public RelNode getRelNode() {
    return baseQuery.getRelNode();
  }

}
