package ai.dataeng.sqml.util;

import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.ComparisonExpression.Operator;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.VersionedName;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqrlNodeUtil {
  /**
   * Unnamed columns are treated as expressions. It must not be an identifier
   */
  public static boolean hasOneUnnamedColumn(Query query) {
    QueryBody body = query.getQueryBody();
    if (body instanceof QuerySpecification) {
      Select select = ((QuerySpecification)body).getSelect();
      if (select.getSelectItems().size() != 1) {
        return false;
      }
      //SELECT *
      if (!(select.getSelectItems().get(0) instanceof SingleColumn)) {
        return false;
      }
      SingleColumn column = (SingleColumn) select.getSelectItems().get(0);
      if (column.getAlias().isPresent()) {
        return false;
      }
      if (column.getExpression() instanceof Identifier) {
        return false;
      }

      return true;
    }
    throw new RuntimeException("not yet implemented");
  }

  public static List<SingleColumn> getSelectList(Query query) {
    QueryBody body = query.getQueryBody();
    if (body instanceof QuerySpecification) {
      Select select = ((QuerySpecification)body).getSelect();
      return select.getSelectItems().stream()
          .map(c->(SingleColumn) c)
          .collect(Collectors.toList());
    }
    throw new RuntimeException("not yet implemented");
  }

  public static OrderBy mapToOrdinal(Select select, OrderBy orderBy) {
    List<SortItem> ordinals = orderBy.getSortItems().stream()
        .map(s->new SortItem(s.getLocation(), new LongLiteral(
            Long.toString(mapToOrdinal(select, s.getSortKey()) + 1)),
            s.getOrdering()))
        .collect(Collectors.toList());

    return new OrderBy(orderBy.getLocation(), ordinals);
  }
  public static GroupBy mapToOrdinal(Select select, GroupBy groupBy) {
    List<Expression> ordinals = mapToOrdinal(select, groupBy.getGroupingElement().getExpressions());
    return new GroupBy(new SimpleGroupBy(ordinals));
  }

  //TODO: This is incorrect logic as we need to match equivalent columns
  public static int mapToOrdinal(Select select, Expression expression) {
    int index = IntStream.range(0, select.getSelectItems().size())
        .filter(i -> {
          SingleColumn column = ((SingleColumn)select.getSelectItems().get(i));
          return
              column.getExpression().equals(expression) ||
                  (column.getAlias().isPresent() && column.getAlias().get().equals(expression));
        })
        .findFirst()
        .orElseThrow(()->
            new RuntimeException("Cannot find element for ordinal: " + expression));
    return index;
  }

  public static List<Expression> mapToOrdinal(Select select, List<Expression> expressions) {
    Set<Integer> grouping = new HashSet<>();
    for (Expression expression : expressions) {
      int index = mapToOrdinal(select, expression);
      grouping.add(index);
    }

    List<Expression> ordinals = grouping.stream()
        .map(i->(Expression)new LongLiteral(Long.toString(i + 1)))
        .collect(Collectors.toList());
    return ordinals;
  }

  public static Identifier ident(Name name) {
    return ident(name.toNamePath());
  }
  public static SingleColumn singleColumn(NamePath name, Name alias) {
    return new SingleColumn(ident(name), ident(alias));
  }

  public static Identifier ident(NamePath name) {
    return new Identifier(Optional.empty(), name);
  }

  public static SelectItem selectAlias(Expression expression, NamePath alias) {
    return new SingleColumn(expression, new Identifier(Optional.empty(), alias));
  }

  public static GroupBy groupBy(List<Expression> grouping) {
    return new GroupBy(new SimpleGroupBy(grouping));
  }

  public static Query query(Select select, Relation relation, GroupBy group) {
    //Build subquery tokens
    QuerySpecification spec = new QuerySpecification(
        Optional.empty(),
        select,
        relation,
        Optional.empty(),
        Optional.of(group),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
    return new Query(spec, Optional.empty(), Optional.empty());
  }

  public static Select select(List<SelectItem> items) {
    return new Select(items);
  }

  public static GroupBy group(List<Expression> identifiers) {
    return new GroupBy(new SimpleGroupBy(identifiers));
  }
  public static Identifier alias(Name alias, Name id) {
    return new Identifier(Optional.empty(), alias.toNamePath().concat(id));
  }
  public static FunctionCall function(NamePath name, Identifier alias) {
    return new FunctionCall(name, List.of(alias), false);
  }
  public static Expression eq(Identifier ident, Identifier ident1) {
    return new ComparisonExpression(Optional.empty(), Operator.EQUAL, ident, ident1);
  }

  public static Expression and(List<Expression> expressions) {
    if (expressions.size() == 0) {
      return null;
    } else if (expressions.size() == 1) {
      return expressions.get(0);
    } else if (expressions.size() == 2) {
      return new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
          expressions.get(0),
          expressions.get(1));
    }

    return new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
        expressions.get(0), and(expressions.subList(1, expressions.size())));
  }

  public static Expression and(Expression expression, Optional<Expression> optionalExpression) {
    if (optionalExpression.isEmpty()) {
      return expression;
    }

    return and(List.of(expression, optionalExpression.get()));
  }

}
