package com.datasqrl.plan.local.transpile;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.name.ReservedName;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Context;
import com.datasqrl.schema.Column;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec.SqrlJoinDeclarationVisitor;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.UnboundJoin;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

/**
 * Walks a query and collects information that is needed for transpilaton
 */
@Getter
public class AnalyzeStatement implements
    SqrlJoinDeclarationVisitor<Context, Context>,
    SqrlJoinTermVisitor<Context, Context> {

  private final SqrlCalciteSchema schema;
  private final List<String> assignmentPath;
  private final Optional<SqlIdentifier> selfIdentifier;
  private final Map<SqlIdentifier, ResolvedTableField> expressions = new HashMap<>();

  public Map<SqlIdentifier, ResolvedTable> tableIdentifiers = new HashMap<>();
  public Map<SqlNode, String> mayNeedAlias = new HashMap<>();
  public Map<SqlSelect, List<SqlNode>> expandedSelect = new HashMap<>();
  public Map<SqlNodeList, List<SqlNode>> groupByExpressions = new HashMap<>();
  public Map<SqlNode, String> tableAlias = new HashMap<>();
  private Map<SqlNode, SqlNode> aliasedOrder = new HashMap<>();
  private boolean allowSystemFields;
  private Optional<SQRLTable> context;

  @Value
  public static class Analysis {

    private SqrlCalciteSchema schema;
    private List<String> assignmentPath;
    private Optional<SqlIdentifier> selfIdentifier;
    private Map<SqlIdentifier, ResolvedTableField> expressions;
    public Map<SqlIdentifier, ResolvedTable> tableIdentifiers;
    public Map<SqlNode, String> mayNeedAlias;
    public Map<SqlSelect, List<SqlNode>> expandedSelect;
    public Map<SqlNodeList, List<SqlNode>> groupByExpressions;
    public Map<SqlNode, String> tableAlias;
    private Map<SqlNode, SqlNode> aliasedOrder;
    private boolean allowSystemFields;
  }

  public AnalyzeStatement(SqrlCalciteSchema schema, List<String> assignmentPath,
      Optional<SQRLTable> context) {
    this(schema, assignmentPath, false, context);
  }

  public AnalyzeStatement(SqrlCalciteSchema schema, List<String> assignmentPath,
      boolean allowSystemFields,
      Optional<SQRLTable> context) {
    this.schema = schema;
    this.assignmentPath = assignmentPath;
    this.allowSystemFields = allowSystemFields;
    this.context = context;
    if (!this.assignmentPath.isEmpty()) {
      this.selfIdentifier = Optional.of(new SqlIdentifier(assignmentPath, SqlParserPos.ZERO));
    } else {
      this.selfIdentifier = Optional.empty();
    }
  }

  public Analysis accept(SqlNode node) {
    Context context = new Context();
    visit(node, context);
    return new Analysis(schema,
        assignmentPath,
        selfIdentifier,
        expressions,
        tableIdentifiers,
        mayNeedAlias,
        expandedSelect,
        groupByExpressions,
        tableAlias,
        aliasedOrder,
        allowSystemFields);
  }

  //No contextful single dispatch in SqlNode so we do it manually
  private Context visit(SqlNode node, Context context) {
    switch (node.getKind()) {
      case IDENTIFIER:
        return visitFrom((SqlIdentifier) node, context);
      case AS:
        return visitAliasedRelation((SqlCall) node, context);
      case SELECT:
        return visitSelect((SqlSelect) node, context);
      case UNION:
        return visitSetOperation((SqlCall) node, context);
      case JOIN:
        return visitJoin((SqlJoin) node, context);
      case JOIN_DECLARATION:
        SqrlJoinDeclarationSpec spec = (SqrlJoinDeclarationSpec) node;
        return spec.accept(this, context);
      default:
        throw new SqrlAstException(ErrorCode.GENERIC_ERROR, node.getParserPosition(),
            "unknown ast node");
    }
  }

  @Override
  public Context visitJoinDeclaration(SqrlJoinDeclarationSpec node, Context context) {
    Context relContext = node.getRelation().accept(this, context);

    //Also get expanded left joins
    if (node.getLeftJoins().isPresent()) {
      for (SqlNode sqlNode : node.getLeftJoins().get().getList()) {
        UnboundJoin join = (UnboundJoin) sqlNode;
        relContext = combine(relContext, visit(join.getRelation(), relContext));

        //Walk condition
        if (join.getCondition().isPresent()) {
          visitExpr(join.getCondition().get(), relContext, false);
        }
      }
    }

    final Context finalContext = relContext;

    node.getOrderList().ifPresent(order -> order.getList()
        .forEach(o -> visitExpr(o, finalContext, true)));

    return relContext;
  }

  @Override
  public Context visitJoinPath(SqrlJoinPath node, Context context) {
    Context ctx = context;
    for (int i = 0; i < node.relations.size(); i++) {
      SqlNode rel = node.getRelations().get(i);
      final Context relContext = combine(visit(rel, ctx), ctx);
      ctx = relContext;
      Optional<SqlNode> condition = Optional.ofNullable(node.getConditions().get(i));
      condition.ifPresent(c -> visitExpr(c, relContext, false));
    }

    return ctx;
  }

  @Override
  public Context visitJoinSetOperation(SqrlJoinSetOperation node, Context context) {
    throw new RuntimeException("EXCEPT and INTERSECT not yet supported");
  }

  public Context visitFrom(SqlIdentifier id, Context context) {
    Optional<ResolvedTable> resolve = resolve(id, context);
    if (resolve.isEmpty()) {
      throw new SqrlAstException(
          ErrorCode.MISSING_TABLE, id.getParserPosition(), "Could not find table: " + id.names);
    }

    ResolvedTable resolved = resolve.get();

    String alias = resolved.getTableName()
        .orElseGet(this::generateInaccessibleAlias);

    tableIdentifiers.put(id, resolved);

    mayNeedAlias.put(id, alias); //node may be replaced by aliased relation

    if (id.names.size() == 1 && id.names.get(0)
        .equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      mayNeedAlias.remove(id);
    }

    tableAlias.put(id, alias);
    return newContext(context, aliasFields(resolved.getAccessibleFields(), alias));
  }


  private List<RelationField> toFields(List<String> names) {
    return names.stream()
        //create anonymous column
        .map(f -> new RelationField(new Column(Name.system(f), Name.system(f), 0, true,
            new BasicSqlType(TypeFactory.getTypeSystem(), SqlTypeName.ANY)), null))
        .collect(Collectors.toList());
  }


  private List<RelationField> aliasFields(List<RelationField> accessibleFields, String alias) {
    return accessibleFields.stream()
        .map(f -> new RelationField(f.getField(), alias))
        .collect(Collectors.toList());
  }

  int i = 0;

  private String generateInaccessibleAlias() {
    return "_x" + (++i);
  }


  public Context newContext(Context parent, List<RelationField> fields) {
    return new Context(parent, fields);
  }

  private Optional<ResolvedTable> resolve(SqlIdentifier id, Context context) {

    //_
    Optional<ResolvedTable> selfTable = resolveSelfTable(id, context);
    if (selfTable.isPresent()) {
      return selfTable;
    }

    //o.entries
    Optional<ResolvedTable> relativeTable = resolveRelativeTable(id, context);
    if (relativeTable.isPresent()) {
      return relativeTable;
    }

    //FROM Orders.entries
    Optional<ResolvedTable> table = resolveSchema(id);
    if (table.isPresent()) {
      return table;
    }
    return Optional.empty();
  }

  private Optional<ResolvedTable> resolveSelfTable(SqlIdentifier id, Context context) {
    if (id.names.size() == 1 && id.names.get(0)
        .equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      return Optional.of(
          new SingleTable(ReservedName.SELF_IDENTIFIER.getCanonical(), this.context.get(),
              this.context.get().getFields().getAccessibleFields()));
    }

    return Optional.empty();
  }

  private Optional<ResolvedTable> resolveRelativeTable(SqlIdentifier id, Context context) {
    if (id.names.size() == 1 || context.fields == null) {
      return Optional.empty();
    }
    Optional<RelativeResolvedTable> path = context.resolveTablePath(id.names);

    return path.map(p -> p);
  }

  private Optional<ResolvedTable> resolveSchema(SqlIdentifier id) {
    //look for relschema
    Optional<Table> baseTable = Optional.ofNullable(
            schema.getTable(id.names.get(0), false))
        .map(t -> t.getTable());

    Optional<SQRLTable> base = Optional.empty();
    if (baseTable.isPresent()) {
      //We could resolve either sqrl tables or virtual tables
      // SELF tables are usually anchored on a virtual table so we
      // don't have to expand the relationship.
      if (baseTable.get() instanceof SQRLTable) {
        base = Optional.of((SQRLTable) baseTable.get());
      } else if (baseTable.get() instanceof VirtualRelationalTable) {
        base = Optional.of(((VirtualRelationalTable) baseTable.get()).getSqrlTable());
        Preconditions.checkState(id.names.size() == 1);
        //Virtual tables exposes all fields to the analyzer
        return Optional.of(
            new VirtualResolvedTable(id.names.get(0), (VirtualRelationalTable) baseTable.get()));
      }
    }

    //Also check self but anchor it to a fixed table
    if (base.isEmpty()
        && id.names.get(0).equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      base = selfIdentifier.flatMap(i -> resolveSchema(i)).map(t -> t.getToTable());
    }

    if (base.isEmpty()) {
      return Optional.empty();
    } else if (id.names.size() == 1) {
      return Optional.of(new SingleTable(id.names.get(0), base.get(),
          base.get().getFields().getAccessibleFields()));
    } else {
      Optional<SQRLTable> toTable = base.get()
          .walkTable(NamePath.of(id.names.subList(1, id.names.size()).toArray(new String[]{})));
      if (toTable.isEmpty()) {
        return Optional.empty();
      }

      List<Field> fields = base.get().walkField(id.names.subList(1, id.names.size()));
      if (fields.get(fields.size() - 1) instanceof Relationship) {
        return Optional.of(new AbsoluteResolvedTable(fields.stream()
            .map(e -> (Relationship) e)
            .collect(Collectors.toList())));
      }
    }

    return Optional.empty();
  }

  private List<Field> toVtFields(VirtualRelationalTable vt) {
    List<Field> fields = new ArrayList<>();
    for (RelDataTypeField f : vt.getRowType().getFieldList()) {
      if (vt.getSqrlTable().getField(Name.system(f.getName())).isEmpty()) {
        fields.add(
            new Column(Name.system(f.getName()), Name.system(f.getName()), 0, false, f.getType()));
      }
    }

    fields.addAll(vt.getSqrlTable().getFields().getAccessibleFields());
    return fields;
  }

  @Value
  class VirtualResolvedTable extends ResolvedTable {

    String name;
    VirtualRelationalTable vt;

    public Optional<String> getTableName() {
      return Optional.of(name);
    }

    @Override
    SQRLTable getToTable() {
      return vt.getSqrlTable();
    }

    public List<RelationField> getAccessibleFields() {
      return vt.getRowType().getFieldList().stream()
          .map(f -> new RelationField(
              new Column(Name.system(f.getName()), Name.system(f.getName()), 0, false, f.getType()),
              null))
          .collect(Collectors.toList());
    }
  }

  @Value
  class SingleTable extends ResolvedTable {

    String name;
    SQRLTable table;
    List<Field> fields;

    public Optional<String> getTableName() {
      return Optional.of(name);
    }

    @Override
    SQRLTable getToTable() {
      return table;
    }

    public List<RelationField> getAccessibleFields() {
      return fields.stream()
          .map(f -> new RelationField(f, null))
          .collect(Collectors.toList());
    }
  }

  @Value
  public static class RelativeResolvedTable extends ResolvedTable {

    String alias;
    List<Relationship> fields;

    @Override
    SQRLTable getToTable() {
      return fields.get(fields.size() - 1).getToTable();
    }

    public List<RelationField> getAccessibleFields() {
      return getToTable().getFields().getAccessibleFields().stream()
          .map(f -> new RelationField(f, null))
          .collect(Collectors.toList());
    }
  }


  @Value
  class AbsoluteResolvedTable extends ResolvedTable {

    List<Relationship> fields;

    @Override
    SQRLTable getToTable() {
      return fields.get(fields.size() - 1).getToTable();
    }

    public boolean isPathed() {
      return true;
    }

    public List<RelationField> getAccessibleFields() {
      return getToTable().getFields().getAccessibleFields().stream()
          .map(f -> new RelationField(f, null))
          .collect(Collectors.toList());
    }
  }

  public static abstract class ResolvedTable {

    abstract SQRLTable getToTable();

    public boolean isPathed() {
      return true;
    }

    public List<RelationField> getAccessibleFields() {
      return List.of();
    }

    public Optional<String> getTableName() {
      return Optional.empty();
    }
  }

  public Context visitSetOperation(SqlCall node, Context context) {
    for (SqlNode arm : node.getOperandList()) {
      visit(arm, context);
    }
    return context;
  }

  //Left deep tables
  public Context visitJoin(SqlJoin node, Context context) {
    Context left = visit(node.getLeft(), context);
    Context right = visit(node.getRight(), left);

    Context context1 = combine(left, right);

    if (node.getCondition() != null) {
      visitExpr(node.getCondition(), context1, false);
    }

    return context1;
  }

  private Context combine(Context left, Context right) {
    List<RelationField> fields = new ArrayList<>(left.fields);
    if (right.fields != null) {
      fields.addAll(right.fields);
    }

    return new Context(null, fields);
  }

  public Context visitAliasedRelation(SqlCall node, Context context) {
    String alias = ((SqlIdentifier) node.getOperandList().get(1)).names.get(0);

    Context rel = visit(node.getOperandList().get(0), context);
    mayNeedAlias.remove(node.getOperandList().get(0));

    tableAlias.put(node.getOperandList().get(0), alias);
    return rel.aliasIdentifiers(alias);
  }

  public Context visitSelect(SqlSelect select, Context context) {
    //subquery or top

    Context relationCtx = visit(select.getFrom(), context);

    if (select.getWhere() != null) {
      visitWhere(select.getWhere(), relationCtx);
    }

    List<SqlNode> expandedSelect = visitSelectList(select, relationCtx);

    if (select.getGroup() != null && !select.getGroup().getList().isEmpty()) {
      visitGroupBy(select, relationCtx, expandedSelect);
    }
    if (select.getHaving() != null) {
      visitHaving(select, relationCtx);
    }

    if (select.getOrderList() != null) {
      visitOrder(select, relationCtx);
    }

    return newContext(relationCtx, toFields(extractNames(expandedSelect)));
  }

  private List<SqlNode> visitSelectList(SqlSelect select, Context context) {
    //If select *, get all from ctx
    //If aliased select *, get all from that alias
    //If expression, analyze expression
    List<SqlNode> expandedSelect = new ArrayList<>();

    for (SqlNode node : select.getSelectList().getList()) {
      if (node instanceof SqlIdentifier) {
        SqlIdentifier identifier = (SqlIdentifier) node;
        if (identifier.isStar()) {
          List<SqlIdentifier> star = resolveStar(identifier, context);
          expandedSelect.addAll(star);
          star.stream()
              .forEach(n -> visitExpr(n, context, false));

          continue;
        }
      }

      expandedSelect.add(node);
      visitExpr(node, context, true);
    }

    this.expandedSelect.put(select, expandedSelect);
    return expandedSelect;
  }

  private void visitGroupBy(SqlSelect select, Context context, List<SqlNode> selectList) {
    List<SqlNode> groupByExpression = new ArrayList<>();
    List<String> selectNames = extractNames(selectList);
    List<SqlNode> selectExpressions = stripAs(selectList);
    List<SqlNode> list = select.getGroup().getList();
    outer:
    for (int j = 0; j < list.size(); j++) {
      SqlNode groupItem = list.get(j);
      if (groupItem instanceof SqlLiteral &&
          ((SqlLiteral) groupItem).getTypeName() == SqlTypeName.BIGINT) {
        throw new SqrlAstException(ErrorCode.ORDINAL_NOT_SUPPORTED, groupItem.getParserPosition(),
            "Ordinals not supported in group by");
      }

      //SQRL allows using column names in group by statements
      if (groupItem instanceof SqlIdentifier && ((SqlIdentifier) groupItem).names.size() == 1 &&
          selectNames.contains(((SqlIdentifier) groupItem).names.get(0))) {
        int index = selectNames.indexOf(((SqlIdentifier) groupItem).names.get(0));
        groupByExpression.add(selectExpressions.get(index));
        continue;
      }

      //Lookup expression in the select list
      for (int k = 0; k < selectExpressions.size(); k++) {
        if (selectExpressions.get(k).equalsDeep(groupItem, Litmus.IGNORE)) {
          groupByExpression.add(selectExpressions.get(k));
          continue outer;
        }
      }

      throw new SqrlAstException(ErrorCode.MISSING_FIELD, groupItem.getParserPosition(),
          String.format("Could not find grouping key: %s", groupItem));
    }

    groupByExpression.stream()
        .forEach(expr -> visitExpr(expr, context, true));

    this.groupByExpressions.put(select.getGroup(), groupByExpression);
    //walk group by and pair with select list.
  }

  private List<SqlNode> stripAs(List<SqlNode> selectList) {
    return selectList.stream()
        .map(s -> SqlUtil.stripAs(s))
        .collect(Collectors.toList());
  }

  private List<String> extractNames(List<SqlNode> selectList) {
    return IntStream.range(0, selectList.size())
        .mapToObj(i -> SqlValidatorUtil.getAlias(selectList.get(i), i))
        .collect(Collectors.toList());
  }

  private void visitHaving(SqlSelect select, Context context) {
    //no paths so we don't need to walk
  }

  private void visitOrder(SqlSelect select, Context context) {
    // Check to see if its in the select list. If so, then no op
    // If not, add as a unique order item and analyze the expression

    List<SqlNode> orderByExpression = new ArrayList<>();
    List<String> selectNames = extractNames(select.getSelectList().getList());
    List<SqlNode> selectExpressions = stripAs(select.getSelectList().getList());
    List<SqlNode> list = select.getOrderList().getList();
    outer:
    for (int j = 0; j < list.size(); j++) {
      SqlNode orderItemCall = list.get(j);
      SqlNode orderItem = orderItemCall.getKind() == SqlKind.DESCENDING
          ? ((SqlCall) orderItemCall).getOperandList().get(0)
          : orderItemCall;

      if (orderItem instanceof SqlLiteral &&
          ((SqlLiteral) orderItem).getTypeName() == SqlTypeName.BIGINT) {
        throw new SqrlAstException(ErrorCode.ORDINAL_NOT_SUPPORTED, orderItem.getParserPosition(),
            "Ordinals not supported in order by");
      }

      //SQRL allows using column names in order statements
      if (orderItem instanceof SqlIdentifier && ((SqlIdentifier) orderItem).names.size() == 1 &&
          selectNames.contains(((SqlIdentifier) orderItem).names.get(0))) {
        int index = selectNames.indexOf(((SqlIdentifier) orderItem).names.get(0));
        orderByExpression.add(selectExpressions.get(index));
      } else {
        visitExpr(orderItem, context, true);
      }

    }

  }

  private List<SqlIdentifier> resolveStar(SqlIdentifier id, Context relationCtx) {
    if (id.names.size() > 1) {
      return relationCtx.resolvePrefixScalars(id.names.get(0));
    }

    return relationCtx.resolveAllScalars();
  }

  @Value
  public static class RelationField {

    //todo parser position
    Field field;
    String alias;

    public SqlIdentifier toIdentifier() {
      return new SqlIdentifier(List.of(alias, field.getName().getCanonical()),
          SqlParserPos.ZERO);
    }

    public RelationField withAlias(String alias) {
      return new RelationField(field, alias);
    }
    //etc
  }

  private void visitWhere(SqlNode where, Context context) {
    visitExpr(where, context, true);
  }

  public void visitExpr(SqlNode expr, Context context, boolean allowPaths) {

    AnalyzeExpression analyzeExpression = new AnalyzeExpression(allowPaths, context,
        allowSystemFields, this);
    expr.accept(analyzeExpression);
    expressions.putAll(analyzeExpression.getResolvedFields());
  }

  public static class Context {

    public Context() {
    }

    public Context(Context parent, List<RelationField> fields) {
      this.parent = parent;
      this.fields = fields;
    }

    Context parent;
    List<RelationField> fields;

    public Context aliasIdentifiers(String alias) {
      List<RelationField> fieldList = fields.stream()
          .map(f -> f.withAlias(alias))
          .collect(Collectors.toList());

      //return all fields but with new aliased
      return new Context(parent, fieldList);
    }

    public List<SqlIdentifier> resolvePrefixScalars(String prefix) {
      return fields.stream()
          .filter(f -> f.alias.equalsIgnoreCase(prefix))
          .filter(f -> f.field instanceof Column)
          .filter(f -> f.field.isVisible())
          .map(f -> f.toIdentifier())
          .collect(Collectors.toList());
    }

    public List<SqlIdentifier> resolveAllScalars() {
      return fields.stream()
          .filter(f -> f.field instanceof Column)
          .filter(f -> f.field.isVisible())
          .map(f -> f.toIdentifier())
          .collect(Collectors.toList());
    }

    /**
     * Alias is required or its invalid
     */
    public Optional<RelativeResolvedTable> resolveTablePath(List<String> path) {
      Preconditions.checkState(path.size() > 1);
      //Find the table in the scope then walk the remaining
      Optional<RelationField> base = Optional.empty();
      for (RelationField field : this.fields) {
        if (field.alias.equalsIgnoreCase(path.get(0)) &&
            field.field.getName().getCanonical().equalsIgnoreCase(path.get(1))) {
          if (!(field.field instanceof Relationship)) {
            return Optional.empty();
          }
          base = Optional.of(field);
          break;
        }
      }

      if (base.isEmpty()) {
        return Optional.empty();
      }

      Relationship rel = (Relationship) base.get().field;
      List<Relationship> rels = new ArrayList<>();
      rels.add(rel);
      for (int i = 2; i < path.size(); i++) {
        Optional<Field> field = rels.get(rels.size() - 1).getToTable()
            .getField(Name.system(path.get(i)));
        if (field.isEmpty() || !(field.get() instanceof Relationship)) {
          throw new RuntimeException("Could not table resolve at: " + path);
        }
        rels.add((Relationship) field.get());
      }

      return Optional.of(new RelativeResolvedTable(base.get().alias, rels));
    }

    public Optional<ResolvedTableField> resolveField(SqlNode node, ImmutableList<String> names) {
      //check if fully qualified or has an alias, preference for fully qualified

      if (names.size() > 1) {
        for (RelationField field : this.fields) {
          if (field.getAlias().equalsIgnoreCase(names.get(0)) &&
              field.getField().getName().getCanonical().equalsIgnoreCase(names.get(1))
          ) {
            //found a field, walk or fail
            return walkRemaining(field, names.subList(2, names.size()));
          }

        }
      }

      //Check for unqualified field
      for (RelationField field : this.fields) {
        if (field.getField().getName().getCanonical().equalsIgnoreCase(names.get(0))
        ) {
          //found a field, walk or fail
          return walkRemaining(field, names.subList(1, names.size()));
        }
      }

      return Optional.empty();
    }


    private Optional<ResolvedTableField> walkRemaining(RelationField field,
        ImmutableList<String> subList) {
      List<Field> remainingFields = new ArrayList<>();
      remainingFields.add(field.field);

      for (int j = 0; j < subList.size(); j++) {
        String name = subList.get(j);
        Field cur = remainingFields.get(remainingFields.size() - 1);
        if (!(cur instanceof Relationship) && j != subList.size() - 1) {
          return Optional.empty();
        }
        Relationship rel = (Relationship) cur;
        Optional<Field> found = rel.getToTable().getField(Name.system(name));
        if (found.isEmpty()) {
          return Optional.empty();
        }
        remainingFields.add(found.get());
      }

      return Optional.of(new ResolvedTableField(field.alias, remainingFields));
    }
  }

  public static class ResolvedTableField {

    @Getter
    String alias;
    @Getter
    List<Field> path;
    SqlIdentifier identifier;

    public ResolvedTableField(String alias, List<Field> path) {
      this.alias = alias;
      this.path = path;
    }

    private SqlIdentifier createAliasedIdentifier(SqlIdentifier original) {
      List<String> names = new ArrayList<>();
      names.add(alias);
      path.stream().map(f -> f.getName().getDisplay())
          .forEach(names::add);

      return new SqlIdentifier(names, original.getParserPosition());
    }

    public SqlIdentifier getAliasedIdentifier(SqlIdentifier original) {
      if (identifier == null) {
        this.identifier = createAliasedIdentifier(original);
      }
      return identifier;
    }

    public SqlNode getShadowedIdentifier(SqlIdentifier original) {
      List<String> names = new ArrayList<>();
      names.add(alias);
      Preconditions.checkState(path.size() == 1);
      Preconditions.checkState(path.get(0) instanceof Column);
      names.add(((Column) path.get(0)).getShadowedName().getDisplay());

      return new SqlIdentifier(names, original.getParserPosition());
    }
  }
}
