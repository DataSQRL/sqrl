package ai.datasqrl.graphql.execution;

import ai.datasqrl.graphql.execution.page.PageProvider;
import ai.datasqrl.graphql.execution.query.BoundSqlQuery;
import ai.datasqrl.graphql.execution.query.PreparedQueryBuilder;
import ai.datasqrl.graphql.execution.SqlClientProvider;
import ai.datasqrl.graphql.execution.query.H2SingleQuery;
import ai.datasqrl.graphql.execution.table.TableFieldFetcher;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.Value;

@Value
public class DefaultDataFetcher implements DataFetcher {
  SqlClientProvider sqlClientProvider;
  PageProvider pageProvider;
  TableFieldFetcher tableFieldFetcher;

  @Override
  public Object get(DataFetchingEnvironment environment) throws Exception {
    PreparedQueryBuilder h2 = new PreparedQueryBuilder(tableFieldFetcher.getTable(), pageProvider,
        tableFieldFetcher.getCriteria());
    H2SingleQuery query = h2.build(environment);

    BoundSqlQuery boundSqlQuery = new BoundSqlQuery(
        sqlClientProvider.get(),
        query,
        query.getArgs()
    );

    return boundSqlQuery.execute();
  }
}
