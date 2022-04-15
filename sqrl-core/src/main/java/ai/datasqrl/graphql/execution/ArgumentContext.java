package ai.datasqrl.graphql.execution;

import ai.datasqrl.graphql.execution.table.column.H2Column;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

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
