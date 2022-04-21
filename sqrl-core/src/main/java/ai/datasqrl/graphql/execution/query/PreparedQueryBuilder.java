package ai.datasqrl.graphql.execution.query;

import ai.datasqrl.graphql.execution.ArgumentContext;
import ai.datasqrl.graphql.execution.JdbcArgumentParser;
import ai.datasqrl.graphql.execution.RowMapperBuilder;
import ai.datasqrl.graphql.execution.criteria.Criteria;
import ai.datasqrl.graphql.execution.criteria.CriteriaBuilder;
import ai.datasqrl.graphql.execution.criteria.CriteriaBuilder.CriteriaResult;
import ai.datasqrl.graphql.execution.orderby.H2OrderByProvider;
import ai.datasqrl.graphql.execution.page.PageProvider;
import ai.datasqrl.graphql.execution.table.H2ColumnVisitor2;
import ai.datasqrl.graphql.execution.table.H2Table;
import ai.datasqrl.graphql.execution.table.column.Columns;
import ai.datasqrl.graphql.execution.table.column.H2Column;
import com.google.common.base.Preconditions;
import graphql.com.google.common.collect.ImmutableList;
import graphql.com.google.common.collect.Iterables;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;
import lombok.Value;

//Todo: move to strategies
@Value
public class PreparedQueryBuilder {

  H2Table table;
  PageProvider pageProvider;
  H2OrderByProvider orderByProvider = new H2OrderByProvider();
  Optional<Criteria> criteria;

  public H2SingleQuery build(DataFetchingEnvironment environment) {
    ColumnNameBuilder columnNameBuilder = new ColumnNameBuilder();

    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    builder.append(table.getColumns().accept(columnNameBuilder,
        new ColumnContext(environment)));
    builder.append(String.format(" FROM %s ", table.getName()));

    JdbcArgumentParser jdbcArgumentParser = new JdbcArgumentParser();
    ArgumentContext argumentContext = new ArgumentContext(environment);
    table.getColumns().accept(jdbcArgumentParser, argumentContext);
    List<String> clauseList = argumentContext.getClauseList();

    CriteriaBuilder criteriaBuilder = new CriteriaBuilder(environment);
    //Criteria will be passed in via column of parent
    Optional<CriteriaResult> criteria = this.criteria.map(e -> e.accept(criteriaBuilder, null));

    //Should be same as tuples
    List<String> criteriaList = criteria.map(c -> c.getClauseList()).orElse(List.of());

    //Todo remove if condition, exists here to have a condition on including the where clause
    Optional<Tuple> arguments;
    if (clauseList.isEmpty() && criteriaList.isEmpty()) {
      arguments = Optional.empty();
    } else {
      Iterable itr = Iterables.concat(clauseList, criteriaList);

      builder.append(" WHERE " + String.join(" AND ", itr));

      List<Object> values = criteria.map(c -> c.getValuesList()).orElse(List.of());
      Iterable valuesItr = Iterables.concat(argumentContext.getValuesList(), values);
      List<String> valuesItrList = ImmutableList.copyOf(valuesItr);
      arguments = Optional.of(Tuple.from(valuesItrList));
    }

    Optional<List<String>> orderBy = orderByProvider.getOrderBy(environment, table.getColumns());
    if (orderBy.isPresent()) {
      builder.append(String.format(" ORDER BY %s ", String.join(", ", orderBy.get())));
    }

    Optional<Integer> pageSize = pageProvider.parsePageSize(environment);
    if (pageSize.isPresent()) {
      builder.append(" LIMIT " + pageSize.get());
    }

    if (pageProvider.pageState(environment).isPresent()) {
      builder.append(" OFFSET " + pageProvider.pageState(environment).get());
    }

    System.out.println(builder);
    System.out.println(arguments);
    String query = builder.toString();
    RowMapperBuilder rowMapperBuilder = new RowMapperBuilder(pageProvider);

    return new H2SingleQuery(
        query,
        arguments,
        ((Function<RowSet<Row>, Object>) table.getColumns()
            .accept(rowMapperBuilder, null))

    );
  }

  @Value
  public class ColumnNameBuilder extends H2ColumnVisitor2<String, Object> {

    StringJoiner joiner = new StringJoiner(", ");

    @Override
    public String visitColumns(Columns column, Object context) {
      for (H2Column c : column.getColumns()) {
        Preconditions.checkNotNull(c, "Column should not be null");
        c.accept(this, context);
      }

      return joiner.toString();
    }

    @Override
    public String visitH2Column(H2Column column, Object context) {
      joiner.add(column.getPhysicalName());
      return null;
    }
  }

  @Value
  public class ColumnContext {

    DataFetchingEnvironment environment;
  }
}
