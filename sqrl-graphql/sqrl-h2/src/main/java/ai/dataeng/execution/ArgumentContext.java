package ai.dataeng.execution;

import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

@Getter
public class ArgumentContext {
  DataFetchingEnvironment environment;

  public ArgumentContext(DataFetchingEnvironment environment) {
    this.environment = environment;
  }

  //Pairs
  List<H2Column> columnList = new ArrayList<>();
  List<String> clauseList = new ArrayList<>();
  List<Object> valuesList = new ArrayList<>();

  public void addArgument(H2Column column, String clause, Object value) {
    columnList.add(column);
    clauseList.add(clause);
    valuesList.add(value);
  }

}
