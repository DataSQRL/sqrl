package ai.datasqrl.transform;

import static ai.datasqrl.parse.util.SqrlNodeUtil.ident;

import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.name.Name;
import java.util.List;
import java.util.Optional;

public class AliasFirstColumn {
  public Query transform(Query query, Name name) {
    if (query.getQueryBody() instanceof QuerySpecification) {
      QuerySpecification spec = (QuerySpecification) query.getQueryBody();
      SingleColumn column = (SingleColumn)spec.getSelect().getSelectItems().get(0);
      SingleColumn aliased = new SingleColumn(column.getLocation(), column.getExpression(),
          Optional.of(ident(name)));

      return new Query(
          query.getLocation(),
          new QuerySpecification(
              spec.getLocation(),
              new Select(spec.getSelect().getLocation(), spec.getSelect().isDistinct(),
                  List.of(aliased)),
              spec.getFrom(),
              spec.getWhere(),
              spec.getGroupBy(),
              spec.getHaving(),
              spec.getOrderBy(),
              spec.getLimit()
          ),
          query.getOrderBy(),
          query.getLimit()
      );
    } else {
      throw new RuntimeException("not yet implemented");
    }
  }
}
