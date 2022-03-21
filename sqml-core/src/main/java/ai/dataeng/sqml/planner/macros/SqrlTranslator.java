package ai.dataeng.sqml.planner.macros;

import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.and;
import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.eq;
import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.getSiblingCall;
import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.getTableForCall;
import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.ident;
import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.join;
import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.toCall;
import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.unwrapCall;

import ai.dataeng.sqml.planner.AliasGenerator;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.FieldPath;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.SelfField;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.TableField;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SpecialSqlFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlValidator;
import org.checkerframework.checker.nullness.qual.Nullable;

@AllArgsConstructor
public class SqrlTranslator extends BaseSqrlTranslator {
  @Getter
  SqlValidator validator;
  ValidatorProvider validatorProvider;
  Planner planner;
  Optional<Table> contextTable;

  final Multimap<SqrlCalciteTable, SqlCall> aggs = ArrayListMultimap.create();
  final Multimap<FieldPath, SqlIdentifier> toOneMapping = HashMultimap.create();
  final Multimap<SqrlCalciteTable, FieldPath> toOneFields = HashMultimap.create();
  final Map<SqlNode, SqlNode> mapper = new HashMap<>();
  final AliasGenerator gen = new AliasGenerator();
  final AtomicBoolean hasContext = new AtomicBoolean();

  public List<Field> visitQuery(SqlSelect query, Scope scope) {
    transformDistinct(query);
    scope = new Scope(query);
    this.aggs.putAll(analyzeToManyAggs(query));
    analyzeToOneFields(query);

    Scope fromScope = visitFrom(query.getFrom(), scope);
    query.setFrom(fromScope.node);

    query.setSelectList(rewriteSelectItems(query, fromScope));

    query.setWhere(rewriteWhere(query.getWhere(), fromScope));
    query.setGroupBy(rewriteGroupBy(query.getGroup(), query, fromScope));
    query.setHaving(rewriteHaving(query.getHaving(), fromScope));
    query.setOrderBy(rewriteOrderBy(query.getOrderList(), query, fromScope));
    query.setFetch(rewriteFetch(query.getFetch(), fromScope));
    query.setOffset(rewriteOffset(query.getOffset(), fromScope));

    //A new validator is needed because we can't clear out old scopes in the current validator.
    resetValidator();

    try {
      validator.validate(query);
      System.out.println("Validated: " + query);
    } catch (Exception e) {
      System.out.println("Failed to validate: " + query);
      throw e;
    }

    List<Field> fields = FieldFactory.createFields(validator, query.getSelectList(), query.getGroup());

    return fields;
  }

  private void transformDistinct(SqlSelect query) {
    if (query.isDistinct()) {
      query.setOperand(0, SqlNodeList.EMPTY);
      query.setGroupBy(query.getSelectList());
    }
  }

  @Override
  public Scope visitTable(SqlBasicCall from, Scope scope) {
    SqrlCalciteTable tbl = getTableForCall(validator, from);
    detectContext(tbl);

    List<Field> fields = tbl.getFieldPath().getFields();

    String currentAlias = (fields.get(0) instanceof SelfField) ? "_" :
        fields.size() == 1 ? ((SqlIdentifier) from.getOperandList().get(1)).toString() :
            gen.nextTableAlias();

    SqlNode current = toCall(getTable(fields.get(0)), currentAlias);

    //Expand the middle with generated aliases
    for (int i = 1; i < fields.size(); i++) {
      Field field = fields.get(i);
      if (i == fields.size() - 1) {
        SqlIdentifier alias = (SqlIdentifier)from.getOperandList().get(1);

        current = join(JoinType.INNER, current,
            toCall(getTable(fields.get(fields.size() - 1)), alias.names.get(0)), getCondition(field, currentAlias, alias.names.get(0)));
        currentAlias = alias.names.get(0);
      }
      //Create an aliased join
      else {
        String nextAlias = gen.nextTableAlias();
        current = join(JoinType.INNER, current, toCall(getTable(field), nextAlias), getCondition(field, currentAlias, nextAlias));
        currentAlias = nextAlias;
      }
    }

    Scope expanded = expandFields(tbl, currentAlias, current);

    return expanded;
  }

  private SqlNode getCondition(Field field, String currentAlias, String nextAlias,
      @Nullable SqlNode condition) {
    if (field instanceof Relationship) {
      SqlNode idCondition = getCondition(field, currentAlias, nextAlias);
      if (condition != null) {
        return and(List.of(condition, idCondition));
      }
    }
    return condition;
  }
  private SqlNode getCondition(Field field, String currentAlias, String nextAlias) {
    if (field instanceof Relationship) {
      return getCondition((Relationship) field, currentAlias, nextAlias);
    }

    return null;
  }
  private SqlNode getCondition(Relationship rel, String currentAlias, String nextAlias) {
    List<SqlNode> nodes = new ArrayList<>();
    Table table = rel.getTable();
    if (rel.getType() == Type.PARENT) {
      table = rel.getToTable();
    }

    for (Column column : table.getPrimaryKeys()) {
      Field fk = rel.getToTable().getField(column.getName());
      String lhs = rel.getPkNameMapping().get((Column) fk);
      String rhs = rel.getPkNameMapping().get(column);
      nodes.add(eq(
          ident(currentAlias, lhs != null ? lhs : column.getId()),
          ident(nextAlias, rhs != null ? rhs : column.getId())));
    }


    return and(nodes);
  }

  @Override
  public Scope visitJoin(SqlJoin join, Scope scope) {
    SqlBasicCall candidateCall = unwrapCall(join.getRight());
    SqrlCalciteTable tbl = getTableForCall(validator, candidateCall);
    detectContext(tbl);

    SqlBasicCall sibCall = getSiblingCall(join.getLeft());
    SqlIdentifier firstAlias = (SqlIdentifier)sibCall.getOperandList().get(1);

    String currentAlias = firstAlias.names.get(0);
    SqlNode current = visitFrom(join.getLeft(), scope).node;
    List<Field> fields = tbl.getFieldPath().getFields();

    //Expand the middle with generated aliases
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);

      //Carry over the join type for the first table
      if (i == 0) {
        //Do not add alias to Table declarations
        String nextAlias;
        SqlNode condition;
        if (fields.size() == 1) {
          SqlIdentifier alias = (SqlIdentifier)candidateCall.getOperandList().get(1);
          nextAlias = alias.names.get(0);
          condition = join.getCondition();
        } else {
          nextAlias = gen.nextTableAlias();
          condition = getCondition(field, currentAlias, nextAlias);
        }

        current = join(join.getJoinType(), current, toCall(getTable(field), nextAlias),
            condition);
        currentAlias = nextAlias;
      }
      //Carry over the condition if last table
      else if (i == fields.size() - 1) {
        SqlIdentifier alias = (SqlIdentifier)candidateCall.getOperandList().get(1);

        current = join(JoinType.INNER, current, toCall(getTable(fields.get(fields.size() - 1)),
            alias.names.get(0)), join.getCondition());
        currentAlias = alias.names.get(0);
      }
      //Create an aliased join
      else {
        String nextAlias = gen.nextTableAlias();

        current = join(JoinType.INNER, current, toCall(getTable(field), nextAlias), getCondition(field,
            currentAlias, nextAlias));
        currentAlias = nextAlias;
      }
    }

    return expandFields(tbl, currentAlias, current);
  }

  @Override
  public Scope visitSubquery(SqlBasicCall subquery, Scope scope) {
    new SqrlTranslator(validator, validatorProvider, planner, contextTable)
        .visitQuery(subquery.operand(0), scope);//todo scope
    return new Scope(subquery);
  }

  private SqlNode getTable(Field field) {
    if (field instanceof SelfField) {
      return new SqlIdentifier(List.of(field.getTable().getId()), SqlParserPos.ZERO);
    } else if (field instanceof Relationship) {
      if (((Relationship) field).getSqlNode() != null) {
        return validatorProvider.create().validate(((Relationship) field).getSqlNode());
      }
      return new SqlIdentifier(List.of(((Relationship)field).getToTable().getId()), SqlParserPos.ZERO);
    } else if (field instanceof TableField) {
      return new SqlIdentifier(List.of(field.getTable().getId()), SqlParserPos.ZERO);
    }
    throw new RuntimeException("Not a table:" + field);
  }

  private void detectContext(SqrlCalciteTable tbl) {
    if (tbl.getFieldPath().getFields().get(0) instanceof SelfField) {
      this.hasContext.set(true);
    }
  }

  private Scope scopeOf(Scope scope, SqlNode node) {
    return new Scope(node);
  }

  private Scope scopeOf(SqlNode node) {
    return new Scope(node);
  }

  private Scope expandFields(SqrlCalciteTable tbl, String baseAlias, SqlNode current) {
    SqlNode toOne = expandToOne(tbl,baseAlias, current);
    SqlNode toMany = expandToMany(tbl, baseAlias, toOne);
    return scopeOf(toMany);
  }

  private SqlNode expandToOne(SqrlCalciteTable tbl, String baseAlias, SqlNode current) {
    for (FieldPath path : toOneFields.get(tbl)) {
      List<Field> fields = path.getFields();

      String tableAlias = baseAlias;//gen.nextTableAlias();

      for (int i = 0; i < fields.size() - 1; i++) {
        tableAlias = gen.nextTableAlias();

        Field field = fields.get(i);
        current = join(JoinType.LEFT, current, toCall(getTable(field), tableAlias), getCondition(field,
            baseAlias, tableAlias));
      }

      //TODO NOT RIGHT: Should be bound to table
      for (SqlIdentifier id : toOneMapping.get(path)) {
        mapper.put(id, new SqlIdentifier(List.of(tableAlias, fields.get(fields.size() - 1).getId()),
            SqlParserPos.ZERO));
      }
    }

    return current;
  }

  private SqlNode expandToMany(SqrlCalciteTable tbl, String baseAlias, SqlNode current) {
    for (SqlCall call : aggs.get(tbl)) {

      String tableAlias = gen.nextTableAlias();
      String fieldAlias = gen.nextAlias();

      String fieldName = ((SqlIdentifier)call.getOperandList().get(0)).names.get(1);
      FieldPath fieldPath = tbl.getSqrlTable().getField(NamePath.parse(fieldName))
          .get();

      fieldPath = checkForCountRel(fieldPath);

      String alias = "_";
      SqlNode from = toCall(tbl.getSqrlTable().getId(), alias);
      for (Field field : fieldPath.getFields()) {
        if (field instanceof Relationship) {
          Relationship rel = (Relationship) field;
          String nextAlias = gen.nextTableAlias();

          from = join(JoinType.INNER, from, toCall(expandRel(rel), nextAlias), getCondition(rel,
              alias, nextAlias));
              //relCondition(rel, alias, nextAlias));
          alias = nextAlias;
        }
      }

      SqlCall rewrittenCall = rewriteCall(call, alias, fieldPath);
      SqlCall aliasedCall = toCall(rewrittenCall, fieldAlias);

      List<SqlNode> pks = new ArrayList<>();
      List<SqlNode> conditionList = new ArrayList<>();
      for (Column column : tbl.getSqrlTable().getPrimaryKeys()) {
        String name = column.getName().getCanonical();
        SqlIdentifier identifier = new SqlIdentifier(List.of(baseAlias, name), SqlParserPos.ZERO);
        SqlIdentifier parentTable = new SqlIdentifier(List.of(tableAlias, name), SqlParserPos.ZERO);
        pks.add(identifier);
        conditionList.add(eq(identifier, parentTable));
      }

      SqlNode condition = and(conditionList);

      List<SqlNode> selectList = new ArrayList<>();
      selectList.addAll(pks);
      selectList.add(aliasedCall);

      SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null,
          new SqlNodeList(selectList, SqlParserPos.ZERO), from,
          null, new SqlNodeList(pks, SqlParserPos.ZERO),
          null, null,
          null, null, null, null);

      //Join on main table
      current = join(JoinType.LEFT, current, toCall(select, tableAlias), condition);

      SqlIdentifier newColumn = new SqlIdentifier(List.of(tableAlias, fieldAlias), SqlParserPos.ZERO);

      mapper.put(call, newColumn);
    }

    return current;
  }
//
//  private SqlNode relCondition(Relationship rel, String alias, String nextAlias) {
//    if (rel.getSqlNode() == null) {
//      return eq(ident(alias, "_uuid$0"), ident(nextAlias, "_uuid$0"));
//    }
//
//    Map<Column, String> map = rel.getAliasMapping();
//    List<SqlNode> nodes = new ArrayList<>();
//    for (Column column : rel.getTable().getPrimaryKeys()) {
//      nodes.add(eq(ident(alias, column.getId()), ident(nextAlias, map.get(column))));
//    }
//
//    return and(nodes);
//  }

  private SqlNode expandRel(Relationship rel) {
    if (rel.getSqlNode() == null) {
      return new SqlIdentifier(rel.getToTable().getId(), SqlParserPos.ZERO);
    }
    return validatorProvider.create().validate(rel.getSqlNode());
  }

  /**
   * Checks for aggs that contain only a rel: count(rel)
   */
  private FieldPath checkForCountRel(FieldPath fieldPath) {
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

  private SqlCall rewriteCall(SqlCall call, String alias, FieldPath fieldPath) {
    SqlNode[] operands = {
        new SqlIdentifier(List.of(alias, fieldPath.getLastField().getId()), SqlParserPos.ZERO)
    };

    SpecialSqlFunction fnc = (SpecialSqlFunction) call.getOperator();
    SqlCall newCall = new SqlBasicCall(fnc.getOriginal(), operands, SqlParserPos.ZERO);
    return newCall;
  }

  /**
   * Push aggregates into a select
   */
  private SqlNode pushdownAgg(SqlSelect select, Collection<SqlCall> call) {
    select.setSelectList(new SqlNodeList(call, SqlParserPos.ZERO));
    return select;
  }

  private SqlNode rewriteOffset(SqlNode offset, Scope fromScope) {
    return offset;
  }

  private SqlNode rewriteFetch(SqlNode fetch, Scope fromScope) {
    return fetch;
  }

  private SqlNodeList rewriteOrderBy(SqlNodeList orderList, SqlSelect query,
      Scope fromScope) {
    if (orderList != null && ((SqrlValidator)validator).hasAgg(query.getSelectList())) {
      List<SqlNode> nodes = new ArrayList<>(orderList);
      //get parent primary key for context
      List<Column> primaryKeys = contextTable.get().getPrimaryKeys();
      for (int i = primaryKeys.size() - 1; i >= 0; i--) {
        Column pk = primaryKeys.get(i);
        nodes.add(0, ident("_", pk.getId()));
      }
      return new SqlNodeList(nodes, SqlParserPos.ZERO);
    }

    return orderList;
  }

  private SqlNode rewriteHaving(SqlNode having, Scope fromScope) {
    if (having != null) {
      return having.accept(new SqlNodeMapper(mapper));
    }
    return having;
  }

  private SqlNodeList rewriteGroupBy(SqlNodeList group, SqlSelect query,
      Scope fromScope) {
    if (((SqrlValidator)validator).hasAgg(query.getSelectList())) {
      List<SqlNode> nodes = new ArrayList<>();
      if (group != null) {
        nodes.addAll(group.getList());
      }
      List<Column> primaryKeys = this.contextTable.get().getPrimaryKeys();
      for (int i = primaryKeys.size() - 1; i >= 0; i--) {
        Column column = primaryKeys.get(i);
        nodes.add(0, ident("_", column.getId()));
      }
      return (SqlNodeList)new SqlNodeList(nodes, SqlParserPos.ZERO).accept(new SqlNodeMapper(mapper));
    }

    if (group != null) {
      return (SqlNodeList)group.accept(new SqlNodeMapper(mapper));
    }
    return group;
  }

  private SqlNode rewriteWhere(SqlNode where, Scope fromScope) {
    if (where == null) {
      return where;
    }
    return where.accept(new SqlNodeMapper(this.mapper));
  }

  private Multimap<SqrlCalciteTable, SqlCall> analyzeToManyAggs(SqlSelect query) {
    ExtractToManyAggs extract = new ExtractToManyAggs(validator, query);
    query.accept(extract);

    return extract.aggs;
  }

  private void analyzeToOneFields(SqlSelect query) {
    query.accept(new SqlScopedShuttle(validator.getSelectScope(query)) {

      @Override
      public @Nullable SqlNode visit(SqlIdentifier identifier) {
        RelDataType dataType;
        try {
          dataType = getScope().fullyQualify(identifier).namespace.getRowType();
        } catch (Exception e) {
          return super.visit(identifier);
        }
        if (!(dataType instanceof SqrlCalciteTable)) {
          return identifier;
        }
        SqrlCalciteTable table = (SqrlCalciteTable)dataType;
        if (table == null) {
          return super.visit(identifier);
        }
        if (identifier.names.size() < 2) {
          return super.visit(identifier);
        }

        Optional<FieldPath> path = table.getSqrlTable().getField(NamePath.parse(identifier.names.get(1)));
        Optional<Field> toOne = path
            .stream()
            .filter(f->f.getFields().size()>1)
            .flatMap(f->f.getFields().stream())
            .filter(f->f instanceof Relationship)
            .filter(f->!(((Relationship)f).getMultiplicity() == Multiplicity.MANY))
            .findAny();

        if (toOne.isPresent()) {
          toOneFields.put(table, path.get());
          toOneMapping.put(path.get(), identifier);
        }

        return identifier;
      }
    });
  }

  public SqlNodeList rewriteSelectItems(SqlSelect query, Scope scope) {
    SqlNodeList list = (SqlNodeList)query.getSelectList().accept(new SqlNodeMapper(this.mapper));

    List<SqlNode> nodes = new ArrayList<>();
    nodes.addAll(list.getList());
    if (this.hasContext.get()) {
      List<Column> primaryKeys = this.contextTable.get().getPrimaryKeys();
      for (int i = primaryKeys.size() - 1; i >= 0; i--) {
        Column column = primaryKeys.get(i);
        nodes.add(ident("_", column.getId()));
      }
    }
    return new SqlNodeList(nodes, SqlParserPos.ZERO);
  }

  private void resetValidator() {
    this.validator = validatorProvider.create();
  }

  @Value
  public class Scope {
    SqlNode node;

    public Scope(SqlNode node) {
      this.node = node;
    }
  }
}
