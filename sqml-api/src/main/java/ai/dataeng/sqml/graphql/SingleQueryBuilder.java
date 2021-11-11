package ai.dataeng.sqml.graphql;

import graphql.schema.DataFetchingEnvironment;
import lombok.Value;

@Value
public class SingleQueryBuilder {
  Table3 table;

  public SingleQuery build(DataFetchingEnvironment environment) {
    return new SingleQuery();
  }
}
