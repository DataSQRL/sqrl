package ai.dataeng.execution;

import ai.dataeng.execution.page.PageProvider;
import ai.dataeng.execution.query.BatchQueryBuilder;
import ai.dataeng.execution.query.BoundSqlQuery;
import ai.dataeng.execution.query.H2SingleQuery;
import ai.dataeng.execution.table.H2Table;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import lombok.Value;
import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

//TODO: wip
@Value
public class BatchDataFetcher {
  DataLoaderRegistry dataLoaderRegistry;
  SqlClientProvider sqlClientProvider;
  PageProvider pageProvider;
  H2Table table;

  public DataFetcher create() throws Exception {
    BatchLoader<Object, Object> batchLoader = environments -> {

      BatchQueryBuilder h2 = new BatchQueryBuilder(table, pageProvider);
      H2SingleQuery query = h2.build(environments);

      BoundSqlQuery boundSqlQuery = new BoundSqlQuery(
          sqlClientProvider.get(),
          query,
          query.getArgs()
      );

      return boundSqlQuery.execute();
//          .thenApply(f->{
//            return pageProvider.wrap((List)f, "abc", true);
//          });
    };

    DataLoader<Object, Object> batchDL = DataLoader.newDataLoader(batchLoader);
    DataFetcher dataFetcherThatCallsTheDataLoader = new DataFetcher() {
      @Override
      public Object get(DataFetchingEnvironment environment) {
        Map<String, Object> m = environment.getSource();

        return environment.getDataLoader("entries")
            .load(environment);
      }
    };

    dataLoaderRegistry.register("entries", batchDL);

    return dataFetcherThatCallsTheDataLoader;
  }

}
