package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.VersionedName;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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


  public static Identifier ident(Name name) {
    return ident(name.toNamePath());
  }

  public static Identifier ident(NamePath name) {
    return new Identifier(Optional.empty(), name);
  }

  public static List<Expression> aliasMany(List<Column> list, Name baseTableAlias) {
    return list.stream()
        .map(e->new Identifier(Optional.empty(), baseTableAlias.toNamePath().concat(e.getId())))
        .collect(Collectors.toList());
  }

  public static SelectItem selectAlias(Expression expression, NamePath alias) {
    return new SingleColumn(expression, new Identifier(Optional.empty(), alias));
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

  public static List<SelectItem> selectAlias(List<Column> list,
      Name baseTableAlias, AliasGenerator gen) {
    return toIdentifier(list, baseTableAlias).stream()
        .map(e->new SingleColumn(e, new Identifier(Optional.empty(), gen.nextAliasName().toNamePath())))
        .collect(Collectors.toList());
  }
  public static List<SelectItem> selectAlias(List<Column> list,
      Name baseTableAlias) {
    return toIdentifier(list, baseTableAlias).stream()
        .map(e->new SingleColumn(e))
        .collect(Collectors.toList());
  }

  private static List<Expression> toIdentifier(List<Column> list, Name baseTableAlias) {
    return list.stream()
        .map(c->new Identifier(Optional.empty(), baseTableAlias.toNamePath().concat(c.getId())))
        .collect(Collectors.toList());
  }

  public static GroupBy group(List<Expression> identifiers) {
    return new GroupBy(new SimpleGroupBy(identifiers));
  }
  public static Identifier alias(Name alias, VersionedName id) {
    return new Identifier(Optional.empty(), alias.toNamePath().concat(id));
  }
  public static FunctionCall function(NamePath name, Identifier alias) {
    return new FunctionCall(name, List.of(alias), false);
  }
}
