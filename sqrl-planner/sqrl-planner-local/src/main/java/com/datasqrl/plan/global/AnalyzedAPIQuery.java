package com.datasqrl.plan.global;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.JdbcParameterHandler;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.queries.APIQuery;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class AnalyzedAPIQuery implements DatabaseQuery {

  private APIQuery baseQuery;
  //TODO: add configuration options for runtime

  public AnalyzedAPIQuery(APIQuery baseQuery) {
    this.baseQuery = baseQuery;
  }

  public SQRLConverter.Config getBaseConfig() {
    return SQRLConverter.Config.builder().build();
  }

  public RelNode getRelNode() {
    return baseQuery.getRelNode();
  }

  @Override
  public IdentifiedQuery getQueryId() {
    return baseQuery;
  }

  @Override
  public RelNode getRelNode(ExecutionStage stage, SQRLConverter sqrlConverter, ErrorCollector errors) {
    return sqrlConverter.convertAPI(this, stage, errors);
  }
}
