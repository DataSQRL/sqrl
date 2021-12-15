package ai.dataeng.execution;

import ai.dataeng.execution.connection.JdbcPool;
import ai.dataeng.execution.page.PageProvider;
import ai.dataeng.execution.query.BoundSqlQuery;
import ai.dataeng.execution.query.H2SingleQuery;
import ai.dataeng.execution.query.PreparedQueryBuilder;
import ai.dataeng.execution.table.H2Table;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import lombok.Value;

@Value
public class DefaultDataFetcher implements DataFetcher {
  JdbcPool jdbcPool;
  PageProvider pageProvider;
  H2Table table;
  @Override
  public Object get(DataFetchingEnvironment environment) throws Exception {
    PreparedQueryBuilder h2 = new PreparedQueryBuilder(table, pageProvider);
    H2SingleQuery query = h2.build(environment);

    BoundSqlQuery boundSqlQuery = new BoundSqlQuery(
        jdbcPool.getSqlClient(),
        query,
        query.getArgs()
    );

    return boundSqlQuery.execute();
  }
}
