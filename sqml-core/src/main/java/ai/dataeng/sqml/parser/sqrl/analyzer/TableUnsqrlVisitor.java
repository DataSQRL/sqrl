package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.SelfField;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.TableField;
import ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.AnalyzerTable;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.ComparisonExpression.Operator;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TableUnsqrlVisitor extends AstVisitor<TableRewriteScope, TableRewriteScope> {

  private final StatementAnalyzer statementAnalyzer;

  public TableUnsqrlVisitor(StatementAnalyzer statementAnalyzer) {
    this.statementAnalyzer = statementAnalyzer;
  }

  @Override
  public TableRewriteScope visitNode(Node node, TableRewriteScope context) {
    throw new RuntimeException("Table type not implemented to unsqrl");
  }

  @Override
  public TableRewriteScope visitTable(TableNode node, TableRewriteScope context) {
    AnalyzerTable table = getAnalyzerTable(node);
    List<Field> fieldPath = table.getFieldPath().getFields();
    Name currentAlias = null;

    Relation current;
    if (context.getCurrent().isPresent()) {
      current = context.getCurrent().get();
    } else if (fieldPath.size() == 1) {
      current = node;
    } else {
      currentAlias = Name.system(statementAnalyzer.gen.nextTableAlias());
      current = new TableNode(Optional.empty(), fieldPath.get(0).name.toNamePath(),
          Optional.of(currentAlias));
    }

    for (int i = 1; i < fieldPath.size(); i++) {
      Field field = fieldPath.get(i);

      Name nextAlias = (i == fieldPath.size() - 1)
          ? node.getAlias().get()
          : Name.system(statementAnalyzer.gen.nextTableAlias());

      Relation lhs = current;
      Relation rhs = toTable(field, nextAlias);
      Optional<Expression> condition = getCondition(field, currentAlias, nextAlias);
      current = join(node.getLocation().get(), Type.INNER, lhs, rhs, condition);

      currentAlias = nextAlias;
    }

    Relation expanded = expandFieldPaths(table, currentAlias, current);

    return new TableRewriteScope(Optional.of(expanded));
  }

  private Relation expandFieldPaths(AnalyzerTable table, Name baseAlias, Relation current) {
    Relation toOne = expandToOne(table, baseAlias, current);
    Relation toMany = expandToMany(table, baseAlias, toOne);
    return toMany;
  }

  private Relation expandToOne(AnalyzerTable tbl, Name baseAlias, Relation current) {
    for (FieldPath path : statementAnalyzer.toOneFields.get(tbl)) {
      List<Field> fields = path.getFields();

      Name tableAlias = baseAlias;

      for (int i = 0; i < fields.size() - 1; i++) {
        tableAlias = Name.system(statementAnalyzer.gen.nextTableAlias());

        Field field = fields.get(i);
        Relation lhs = current;
        Relation rhs = toTable(field, tableAlias);
        Optional<Expression> condition = getCondition(field, baseAlias, tableAlias);
        current = join(null, Type.LEFT, lhs, rhs, condition);
      }

      //TODO NOT RIGHT: Should be bound to table
      for (Identifier id : statementAnalyzer.toOneMapping.get(path)) {
        NamePath name = NamePath.of(tableAlias, fields.get(fields.size() - 1).getId());

        statementAnalyzer.nodeMapper.put(id, new Identifier(Optional.empty(), name));
      }
    }

    return current;
  }

  private Relation expandToMany(AnalyzerTable tbl, Name baseAlias, Relation current) {
    for (FunctionCall call : statementAnalyzer.toManyFields.get(tbl)) {

      Name tableAlias = Name.system(statementAnalyzer.gen.nextTableAlias());
      Name fieldAlias = Name.system(statementAnalyzer.gen.nextAlias());

      NamePath fieldName = ((Identifier) call.getArguments().get(0)).getNamePath();
      FieldPath fieldPath = tbl.getTable().getField(fieldName).get();

      fieldPath = expandToManyField(fieldPath);

      Name alias = Name.system("_");
      Relation from = toTable(new TableField(tbl.getTable()), alias);
      for (Field field : fieldPath.getFields()) {
        if (field instanceof Relationship) {
          Relationship rel = (Relationship) field;
          Name nextAlias = Name.system(statementAnalyzer.gen.nextTableAlias());

          Relation lhs = from;
          Relation rhs = toTable(field, tableAlias);
          Optional<Expression> condition = getCondition(field, baseAlias, tableAlias);
          current = join(null, Type.LEFT, lhs, rhs, condition);

          from = join(null, Type.INNER, lhs, rhs, getCondition(rel,
              alias, nextAlias));
          alias = nextAlias;
        }
      }

      FunctionCall rewrittenCall = rewriteCall(call, alias, fieldPath);

      List<SelectItem> pks = new ArrayList<>();
      List<Expression> conditionList = new ArrayList<>();
      for (Column column : tbl.getTable().getPrimaryKeys()) {
        Name name = column.getName();
        Identifier identifier = new Identifier(Optional.empty(), NamePath.of(baseAlias, name));
        Identifier parentTable = new Identifier(Optional.empty(), NamePath.of(tableAlias, name));
        pks.add(new SingleColumn(identifier));
        conditionList.add(eq(identifier, parentTable));
      }

      Expression condition = and(conditionList);

      List<SelectItem> selectList = new ArrayList<>();
      selectList.addAll(pks);
      selectList.add(
          new SingleColumn(rewrittenCall, new Identifier(Optional.empty(), fieldAlias.toNamePath())));

      QuerySpecification select = new QuerySpecification(
          Optional.empty(),
          new Select(false, selectList),
          from,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

      //Join on main table
      Relation rhs = new TableSubquery(new Query(select, Optional.empty(), Optional.empty()));
      current = join(null, Type.LEFT, current, rhs, Optional.of(condition));

      Identifier newColumn = new Identifier(Optional.empty(), NamePath.of(tableAlias, fieldAlias));

      statementAnalyzer.nodeMapper.put(call, newColumn);
    }

    return current;
  }

  private FunctionCall rewriteCall(FunctionCall call, Name alias, FieldPath fieldPath) {
    return new FunctionCall((Optional<NodeLocation>) null,
        call.getName(),
        List.of(new Identifier(Optional.empty(), NamePath.of(alias, fieldPath.getLastField().getId()))),
        false,
        Optional.empty()
    );
  }

  /**
   * Checks for aggs that contain only a rel: count(rel)
   */
  private FieldPath expandToManyField(FieldPath fieldPath) {
    if (fieldPath.getLastField() instanceof Relationship) {
      Relationship rel = (Relationship) fieldPath.getLastField();
      Column pk = rel.getToTable().getPrimaryKeys().get(0);
      List<Field> fields = new ArrayList<>();
      fields.addAll(fieldPath.getFields());
      fields.add(pk);
      return new FieldPath(fields);
    }

    return fieldPath;
  }

  private Optional<Expression> getCondition(Field field, Name currentAlias, Name nextAlias) {
    if (!(field instanceof Relationship)) {
      return Optional.empty();
    }

    List<Expression> expressions = new ArrayList<>();
    Relationship rel = (Relationship) field;
    Table table = rel.getTable();
    if (rel.getType() == Relationship.Type.PARENT) {
      table = rel.getToTable();
    }

    for (Column column : table.getPrimaryKeys()) {
      Field fk = rel.getToTable().getField(column.getName());
      String lhs = rel.getPkNameMapping().get((Column) fk);
      String rhs = rel.getPkNameMapping().get(column);
      Identifier l = new Identifier(Optional.empty(), NamePath.of(currentAlias, Name.system(lhs)));
      Identifier r = new Identifier(Optional.empty(), NamePath.of(nextAlias, Name.system(rhs)));

      expressions.add(eq(l, r));
//        nodes.add(eq(
//            ident(currentAlias, lhs != null ? lhs : column.getId().toString()),
//            ident(nextAlias, rhs != null ? rhs : column.getId().toString())));
    }

    return Optional.ofNullable(and(expressions));
  }

  public Expression eq(Identifier l, Identifier r) {
    return new ComparisonExpression(Operator.EQUAL, l, r);
  }

  private Expression and(List<Expression> expressions) {
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

  private Relation toTable(Field field, Name nextAlias) {
    if (field instanceof SelfField) {
      return new TableNode(Optional.empty(), field.getTable().getId().toNamePath(),
          Optional.of(nextAlias));
    } else if (field instanceof Relationship) {
      if (((Relationship) field).getSqlNode() != null) {
        //return TableSubquery
        return null;//validatorProvider.create().validate(((Relationship) field).getSqlNode());
      }
//        return new SqlIdentifier(List.of(((Relationship)field).getToTable().getId().toString()), SqlParserPos.ZERO);
    } else if (field instanceof TableField) {
      return new TableNode(Optional.empty(), field.getTable().getId().toNamePath(),
          Optional.of(nextAlias));
    }
    throw new RuntimeException("Not a table:" + field);
  }

  public Join join(NodeLocation location, Type joinType, Relation left, Relation right,
      Optional<Expression> condition) {
    Optional<JoinCriteria> joinCondition = condition.map(c -> new JoinOn(location, c));

    return new Join(location, joinType, left, right, joinCondition);
  }

  @Override
  public TableRewriteScope visitJoin(Join node, TableRewriteScope context) {
    TableRewriteScope leftContext = node.getLeft().accept(this, context);

    leftContext.setPushdownCondition(node.getType(), node.getCriteria());

    TableRewriteScope rightContext = node.getRight().accept(this, leftContext);

    return rightContext;
  }

  private AnalyzerTable getAnalyzerTable(TableNode node) {
    return statementAnalyzer.analyzerTableMap.get(node);
  }
}
