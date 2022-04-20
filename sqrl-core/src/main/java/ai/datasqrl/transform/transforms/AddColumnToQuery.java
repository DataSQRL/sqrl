package ai.datasqrl.transform.transforms;

import static ai.datasqrl.parse.util.SqrlNodeUtil.ident;
import static ai.datasqrl.parse.util.SqrlNodeUtil.singleColumn;

import ai.datasqrl.util.AliasGenerator;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.TableFactory;
import ai.datasqrl.parse.tree.AliasedRelation;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinCriteria;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.name.Name;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AddColumnToQuery {
  AliasGenerator gen = new AliasGenerator();
  /**
   * Adds an expression and joins back to the main query, taking careful account
   * of shadowing.
   *
   * Orders.entries.discount := COALESCE(discount, 0.0);
   *
   * Expression query: SELECT _uuid, _idx1, COALESCE(discount, 0.0) AS discount$1 FROM entries;
   *
   * Transform:
   * SELECT _t1._uuid, _t1._idx1, _t1.quantity, _t1.unit_price, _t1.discount, _t2.discount AS discount$1
   * FROM entries AS _t1
   * INNER JOIN (SELECT _uuid, _idx1, COALESCE(discount, 0.0) AS discount
   *            FROM entries) AS _t2 ON _t1._uuid = _t2._uuid AND _t1._idx1 = _t2._idx1;
   */
  public QuerySpecification transform(Table table, Name expressionName,
      boolean isAggregating, QuerySpecification spec) {
    Name lAlias = gen.nextTableAliasName();
    Map<Name, Table> joinScope = new HashMap<>();
    joinScope.put(lAlias, table);
    TableNode tableNode = new TableNode(Optional.empty(), table.getId().toNamePath(), Optional.of(lAlias));

    Name rAlias = gen.nextTableAliasName();

    Query query = new Query(Optional.empty(), spec, Optional.empty(), Optional.empty());

    Table rhsTable = new TableFactory().create(query);
    joinScope.put(rAlias, rhsTable);
    Type joinType = Type.LEFT;//isAggregating? Type.LEFT : Type.INNER;
    JoinCriteria criteria = JoinWalker.createTableCriteria(joinScope, lAlias, rAlias);

    TableSubquery subquery = new TableSubquery(Optional.empty(), query);
    AliasedRelation relation = new AliasedRelation(subquery, ident(rAlias.toNamePath()));
    Join join = new Join(joinType, tableNode, relation, Optional.of(criteria));

    return new QuerySpecification(
        spec.getLocation(),
        toSelectList(table, expressionName, lAlias, rAlias),
        join,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  private Select toSelectList(Table table, Name expressionName, Name lAlias, Name rAlias) {
    List<SelectItem> selectItems = new ArrayList<>();
    Column c = table.fieldFactory(expressionName);
    SingleColumn column = singleColumn(rAlias.toNamePath().concat(expressionName), c.getId());
    selectItems.add(column);

    for (Field field : table.getFields().getElements()) {
      if (field instanceof Column) {
        selectItems.add(singleColumn(lAlias.toNamePath().concat(field.getName()), field.getId()));
      }
    }

    return new Select(selectItems);
  }
}
