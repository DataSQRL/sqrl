package ai.dataeng.sqml.parser.sqrl.transformers;

import static ai.dataeng.sqml.util.SqrlNodeUtil.ident;

import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SetOperation;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
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
              spec,
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
