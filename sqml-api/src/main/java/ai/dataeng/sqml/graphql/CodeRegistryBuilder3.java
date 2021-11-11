package ai.dataeng.sqml.graphql;

import ai.dataeng.sqml.graphql.BatchQueryBuilder.BatchQuery;
import ai.dataeng.sqml.schema2.TypedField;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import lombok.Value;
import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

@Value
public class CodeRegistryBuilder3 {
  Pool client;
  DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry();

  public GraphQLCodeRegistry build(PhysicalTablePlan plan) {
    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

    for (PlanItem item : plan.getPlan()) {
      TypedField field = item.getField();
      //Need to know what relation this field is hosted on.
      Table3 table = item.getTable();
      codeRegistry.dataFetcher(
          FieldCoordinates.coordinates("Query", field.getCanonicalName()),
          new DataFetcher<CompletionStage<List<Object>>>() {
            @Override
            public CompletionStage<List<Object>> get(DataFetchingEnvironment environment)
                throws Exception {
              SingleQueryBuilder builder = new SingleQueryBuilder(table);
              SingleQuery singleQuery = builder.build(environment);
              PreparedQuery<RowSet<Row>> query = client.preparedQuery(singleQuery.getQuery());

              Future<RowSet<Row>> result;
              if (singleQuery.getArguments().isPresent()) {
                result = query.execute(singleQuery.getArguments().get());
              } else {
                result = query.execute();
              }

              return result
                  .map(singleQuery.getResultMapper()::apply)
                  .toCompletionStage();
            }
          });

    }

    Table3 table = plan.getPlan().get(1).getTable();

    BatchLoader<Object, Object> batchLoader = new BatchLoader<Object, Object>() {
      @Override
      public CompletionStage<List<Object>> load(List<Object> environments) {
        BatchQueryBuilder builder = new BatchQueryBuilder(table);
        BatchQuery batchQuery = builder.build(/*environments*/);
        PreparedQuery<RowSet<Row>> query = client.preparedQuery(batchQuery.getQuery());

        Future<RowSet<Row>> result;
        if (batchQuery.getArguments().isPresent()) {
          result = query.execute(batchQuery.getArguments().get());
        } else {
          result = query.execute();
        }

        return result
            .map(batchQuery.getResultMapper()::apply)
            .toCompletionStage();
      }
    };

    DataLoader<Object, Object> batchDL = DataLoader.newDataLoader(batchLoader);
    DataFetcher dataFetcherThatCallsTheDataLoader = new DataFetcher() {
      int i =0;
      @Override
      public Object get(DataFetchingEnvironment environment) {
        Map<String, Object> m = environment.getSource();

        return environment.getDataLoader("entries").load(++i);
      }
    };
    dataLoaderRegistry.register("entries", batchDL);

    codeRegistry.dataFetcher(
        FieldCoordinates.coordinates("orders", "entries"),
        dataFetcherThatCallsTheDataLoader);

    return codeRegistry.build();
  }
}
