package ai.dataeng.sqml.graphql;

import com.google.common.collect.ArrayListMultimap;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.Value;

@Value
public class BatchQueryBuilder {
  Table3 table;
  QueryBuilder queryBuilder = new QueryBuilder();
  ResultMapper resultMapper = new ResultMapper();
  QueryArgsBuilder queryArgsBuilder = new QueryArgsBuilder();

  public BatchQuery build() {
//    processSelect(table, getSelectionSet(environments));
//    processFrom(table);
//    processWhere(table, getArguments(environments));
//    processLimit(table, environments);

    Object arr[][] = {new Object[]{1}, new Object[]{2}};
    return new BatchQuery(
        "select customerid, entries_pos, discount from entries where (customerid) = ANY(?)",
        Optional.of(Tuple.of(arr)),
      new Function<RowSet<Row>, List<Object>>() {
        @Override
        public List<Object> apply(RowSet<Row> rowSet) {
          ArrayListMultimap<Object, Map> multiMap = ArrayListMultimap.create();

          for (Row r : rowSet) {
            multiMap.put(
                r.getInteger(0),
                Map.of("discount", r.getFloat("discount")));
          }

          //Reorder the results to the original order
          List result = new ArrayList();
            for (Object[] key : arr) {
              result.add(multiMap.get(key[0]));
            }

          return result;
        }
      }
    );
//    return new BatchQuery(queryBuilder.build(), queryArgsBuilder.build(), resultMapper.build());
  }

  private List<DataFetchingEnvironment> getArguments(List<DataFetchingEnvironment> environments) {
    return null;
  }

  private DataFetchingFieldSelectionSet getSelectionSet(
      List<DataFetchingEnvironment> environments) {
    return environments.get(0).getSelectionSet();
  }

  private void processSelect(Table3 table, DataFetchingFieldSelectionSet selectionSet) {
    for (Column3 column : table.getColumns()) {
      if (selectionSet.contains(column.getField().getCanonicalName())) {
        queryBuilder.select(column.getName());
        resultMapper.add(column.getName(), column);
      }
    }

    for (String pk : table.getPrimaryKeys()) {
//      queryBuilder.select(pk.getName());
//      resultMapper.add(pk.getName(), pk);
    }
  }

  private void processWhere(Table3 table, List<DataFetchingEnvironment> environments) {
  }

  private void processFrom(Table3 table) {
    queryBuilder.from(table.getTableName());
  }

  private void processLimit(Table3 table, List<DataFetchingEnvironment> environments) {
  }

  @Value
  public class BatchQuery {
    String query;
    Optional<Tuple> arguments;
    Function<RowSet<Row>, List<Object>> resultMapper;
  }

  class QueryBuilder {
    Set<String> selectColumns = new LinkedHashSet<>();
    String tableName;
    public void select(String name) {
      selectColumns.add(name);
    }

    public void from(String tableName) {
      this.tableName = tableName;
    }

    //Where
    //Limits
    public String build() {
      return String.format("SELECT %s FROM %s", String.join(", ", selectColumns), tableName);
    }
  }

  class ResultMapper {
    final LinkedHashMap<String, Column3> resultMapper = new LinkedHashMap<>();

    public Function<RowSet<Row>, List<Object>> build() {
      return new Function<RowSet<Row>, List<Object>>() {
        @Override
        public List<Object> apply(RowSet<Row> rowSet) {
          ArrayListMultimap<Integer, Map> multiMap = ArrayListMultimap.create();

          for (Row r : rowSet) {
            multiMap.put(
                r.getInteger(0),
                Map.of("id", r.getInteger(0), "productid", r.getInteger(1)));
          }

          List result = new ArrayList();
//          for (Integer key : null) {
//            result.add(multiMap.get(key));
//          }

          return result;
        }
      };
    }

    public void add(String name, Column3 column) {
      resultMapper.put(name, column);
    }
  }

  class QueryArgsBuilder {

    public Optional<Tuple> build() {
      return null;
    }
  }
}
