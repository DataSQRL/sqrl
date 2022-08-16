package ai.datasqrl.plan.local.transpile;

import static org.apache.calcite.util.Static.RESOURCE;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Resolve.StatementOp;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.ConvertableFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SelectNamespace;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

public class ExpressionRewriter extends SqlScopedShuttle {

  @Getter
  private final List<InlineAggExtractResult> inlineAggResults = new ArrayList<>();
  @Getter
  private final List<SqlJoinDeclaration> toOneResults = new ArrayList<>();
  private final Env env;
  private final StatementOp op;

  public ExpressionRewriter(SqlValidatorScope scope, Env env, StatementOp op) {
    super(scope);
    this.env = env;
    this.op = op;
  }

  @Override
  protected SqlNode visitScoped(SqlCall call) {
    //Agg convert
    if (call.getOperator() instanceof ConvertableFunction && !call.getOperator().isAggregator()) {
      return convertToSubquery(call, getScope());
    }

    //Don't walk alias
    if (call.getOperator() == SqrlOperatorTable.AS) {
      return call.getOperandList().get(0).accept(this);
    }

    return super.visitScoped(call);
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    SqlQualified qualified = getScope().fullyQualify(id);
    if (qualified.namespace instanceof SelectNamespace) {
      //Column from subquery
      return id;
    }

    IdentifierNamespace identifierNamespace = (IdentifierNamespace) qualified.namespace;
    SQRLTable st = identifierNamespace.getTable().unwrap(SQRLTable.class);
    List<Relationship> toOneRels = isToOne(qualified.suffix(), st);
    if (!toOneRels.isEmpty()) {
      TableWithPK table = env.getTableMap().get(toOneRels.get(toOneRels.size() - 1).getToTable());
      String newTableAlias = env.getAliasGenerator().generate(table);
      TablePath tablePath = new TablePathImpl(st, Optional.of(qualified.prefix().get(0)), true,
          toOneRels, newTableAlias);
      SqlJoinDeclaration declaration = JoinBuilderImpl.expandPath(tablePath, false,
          () -> new JoinBuilderImpl(env, op));
      this.toOneResults.add(declaration);
      return new SqlIdentifier(List.of(declaration.getLastAlias(), Util.last(qualified.suffix())),
          SqlParserPos.ZERO);
    }

    //not a to-one identifier, get field name and rewrite.
    Optional<Field> fieldOptional = st.getField(Name.system(Util.last(qualified.suffix())));
    if (fieldOptional.isEmpty()) {
      throw this.getScope().getValidator()
          .newValidationError(id, RESOURCE.columnNotFound(Util.last(qualified.suffix())));
    }

    if (fieldOptional.isPresent() && fieldOptional.get() instanceof Relationship) {
      throw this.getScope().getValidator()
          .newValidationError(id, SqrlErrors.SQRL_ERRORS.columnRequiresInlineAgg());
    }

    Column column = (Column) fieldOptional.get();
    String newName = env.getFieldMap().get(column);
    return id.setName(id.names.size() - 1, newName);
  }

  private List<Relationship> isToOne(List<String> suffix, SQRLTable st) {
    if (suffix.size() < 2) {
      return List.of();
    }
    List<Relationship> relationships = new ArrayList<>();
    SQRLTable walk = st;
    for (String name : suffix) {
      Field field = walk.getField(Name.system(name)).get();
      if (field instanceof Relationship) {
        if (((Relationship) field).getMultiplicity() == Multiplicity.MANY) {
          return List.of();
        }
        relationships.add((Relationship) field);
        walk = ((Relationship) field).getToTable();
      }
    }

    return relationships;
  }


  private SqlNode convertToSubquery(SqlCall call, SqlValidatorScope scope) {
    ConvertableFunction function = (ConvertableFunction) call.getOperator();
    SqlIdentifier arg = (SqlIdentifier) call.getOperandList().get(0);
    SqlQualified qualifiedArg = scope.fullyQualify(arg);
    IdentifierNamespace ns = (IdentifierNamespace) qualifiedArg.namespace;
    //p.entires.quantity
    //prefix: p (the thing we need to build the condition for)
    //suffix: entries, quantity (the path)
    //namespace:
    //  resolveNamespace: (TableNamespace, RelativeTableNamespace)
    //   table:relopttable
    //     table: SQRLTable
//    SqlValidatorNamespace tn = scope.getValidator().getNamespace(arg);//ns.getResolvedNamespace();
    SQRLTable baseTable = qualifiedArg.namespace.getTable().unwrap(SQRLTable.class);
    Optional<String> baseAlias = Optional.ofNullable(qualifiedArg.prefix())
        .filter(p -> p.size() == 1).map(p -> p.get(0));

    List<Field> fields = baseTable.walkField(qualifiedArg.suffix());
    List<Relationship> rels = fields.stream().filter(f -> f instanceof Relationship)
        .map(f -> (Relationship) f).collect(Collectors.toList());

    TableWithPK basePkTable = env.getTableMap().get(baseTable);
    String subqAlias = env.getAliasGenerator().generate("s");
    TablePathImpl tablePath = new TablePathImpl(baseTable, baseAlias, false, rels, subqAlias);
    SqlJoinDeclaration declaration = JoinBuilderImpl.expandPath(tablePath, true,
        () -> new JoinBuilderImpl(env, op));

    Preconditions.checkState(declaration.getPullupCondition().isEmpty());

    SqlNode condition = SqlNodeBuilder.createSelfEquality(basePkTable, subqAlias, baseAlias.get());

    //Choose PK if there is no field name
    String fieldName =
        fields.get(fields.size() - 1) instanceof Column ? fields.get(fields.size() - 1).getName()
            .getDisplay()
            : env.getTableMap().get(rels.get(rels.size() - 1).getToTable()).getPrimaryKeys().get(0);

    //Call gets an identifier
    SqlBasicCall newCall = new SqlBasicCall(function.convertToInlineAgg().get(), new SqlNode[]{
        new SqlIdentifier(List.of(declaration.getLastAlias(), fieldName), SqlParserPos.ZERO)},
        call.getParserPosition());

    List<SqlNode> groups = basePkTable.getPrimaryKeys().stream()
        .map(e -> new SqlIdentifier(List.of(declaration.getFirstAlias(), e), SqlParserPos.ZERO))
        .collect(Collectors.toList());

    List<SqlNode> selectList = new ArrayList<>(groups);

    //Add grouping conditions of pk
    //LEFT JOIN (SELECT sq FROM condition) x ON x.a = y.a;
    String fieldAlias = env.getAliasGenerator().generateFieldName();
    SqlBasicCall asField = new SqlBasicCall(SqrlOperatorTable.AS,
        new SqlNode[]{newCall, new SqlIdentifier(fieldAlias, SqlParserPos.ZERO)},
        SqlParserPos.ZERO);
    selectList.add(asField);

    SqlBasicCall as = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{
        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
            new SqlNodeList(selectList, SqlParserPos.ZERO), declaration.getJoinTree(), null,
            new SqlNodeList(groups, SqlParserPos.ZERO), null, null, null, null, null,
            SqlNodeList.EMPTY), new SqlIdentifier(subqAlias, SqlParserPos.ZERO)},
        SqlParserPos.ZERO);

    SqlIdentifier identifier = new SqlIdentifier(List.of(subqAlias, fieldAlias), SqlParserPos.ZERO);
    this.inlineAggResults.add(
        new InlineAggExtractResult(identifier, as, condition, scope.getNode(), qualifiedArg));
    return identifier;
  }

}
