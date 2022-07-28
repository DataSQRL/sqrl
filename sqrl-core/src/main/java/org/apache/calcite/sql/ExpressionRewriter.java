package org.apache.calcite.sql;

import static org.apache.calcite.util.Static.RESOURCE;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;
import ai.datasqrl.plan.local.generate.QueryGenerator.FieldNames;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.ScriptTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.fun.ConvertableFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SelectNamespace;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

public class ExpressionRewriter extends SqlScopedShuttle {

  TableMapperImpl tableMapper;
  UniqueAliasGeneratorImpl uniqueAliasGenerator;
  JoinDeclarationContainerImpl joinDecs;
  SqlNodeBuilderImpl sqlNodeBuilder;
  private final Transpile transpile;
  FieldNames names;
  private final JoinBuilderFactory joinBuilderFactory;
  @Getter
  private List<InlineAggExtractResult> inlineAggResults = new ArrayList<>();
  @Getter
  private List<JoinDeclaration> toOneResults = new ArrayList<>();


  public ExpressionRewriter(SqlValidatorScope initialScope,
      TableMapperImpl tableMapper, UniqueAliasGeneratorImpl uniqueAliasGenerator,
      JoinDeclarationContainerImpl joinDecs, SqlNodeBuilderImpl sqlNodeBuilder, FieldNames names,
      Transpile transpile) {
    super(initialScope);
    this.tableMapper = tableMapper;
    this.uniqueAliasGenerator = uniqueAliasGenerator;
    this.joinDecs = joinDecs;
    this.sqlNodeBuilder = sqlNodeBuilder;
    this.transpile = transpile;
    this.joinBuilderFactory = ()-> new JoinBuilder(uniqueAliasGenerator, joinDecs, tableMapper, sqlNodeBuilder);
    this.names = names;
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

    IdentifierNamespace identifierNamespace = (IdentifierNamespace)qualified.namespace;
    ScriptTable st = identifierNamespace.getTable().unwrap(ScriptTable.class);
    List<Relationship> toOneRels = isToOne(qualified.suffix(), st);
    if (!toOneRels.isEmpty()) {
      TableWithPK table = tableMapper.getTable(toOneRels.get(toOneRels.size()-1).getToTable());
      String newTableAlias = uniqueAliasGenerator.generate(table);
      TablePath tablePath = new TablePathImpl(st, Optional.of(qualified.prefix().get(0)), true, toOneRels, newTableAlias);
      JoinDeclaration declaration = JoinBuilder.expandPath(tablePath, false, ()->new JoinBuilder(uniqueAliasGenerator, joinDecs,
          tableMapper, sqlNodeBuilder));
      this.toOneResults.add(declaration);
      return new SqlIdentifier(List.of(declaration.getLastAlias(), Util.last(qualified.suffix())), SqlParserPos.ZERO);
    }

    //not a to-one identifier, get field name and rewrite.
    Optional<Field> fieldOptional = st.getField(Name.system(Util.last(qualified.suffix())));
    if (fieldOptional.isEmpty()) {
      throw this.getScope().getValidator().newValidationError(id,
          RESOURCE.columnNotFound(Util.last(qualified.suffix())));
    }

    if (fieldOptional.isPresent() && fieldOptional.get() instanceof Relationship) {
      throw this.getScope().getValidator().newValidationError(id,
          SqrlErrors.SQRL_ERRORS.columnRequiresInlineAgg());
    }

    Column column = (Column)fieldOptional.get();
    String newName = names.get(column);
    return id.setName(id.names.size() - 1, newName);
  }

  private List<Relationship> isToOne(List<String> suffix, ScriptTable st) {
    if (suffix.size() < 2) {
      return List.of();
    }
    List<Relationship> relationships = new ArrayList<>();
    ScriptTable walk = st;
    for (String name : suffix) {
      Field field = walk.getField(Name.system(name)).get();
      if (field instanceof Relationship) {
        if (((Relationship)field).getMultiplicity() == Multiplicity.MANY) {
          return List.of();
        }
        relationships.add((Relationship) field);
        walk = ((Relationship)field).getToTable();
      }
    }

    return relationships;
  }


  private SqlNode convertToSubquery(SqlCall call, SqlValidatorScope scope) {
    ConvertableFunction function = (ConvertableFunction)call.getOperator();
    SqlIdentifier arg = (SqlIdentifier)call.getOperandList().get(0);
    SqlQualified qualifiedArg = scope.fullyQualify(arg);
    IdentifierNamespace ns = (IdentifierNamespace)qualifiedArg.namespace;
    //p.entires.quantity
    //prefix: p (the thing we need to build the condition for)
    //suffix: entries, quantity (the path)
    //namespace:
    //  resolveNamespace: (TableNamespace, RelativeTableNamespace)
    //   table:relopttable
    //     table: ScriptTable
//    SqlValidatorNamespace tn = scope.getValidator().getNamespace(arg);//ns.getResolvedNamespace();
    ScriptTable baseTable = qualifiedArg.namespace.getTable().unwrap(ScriptTable.class);
    Optional<String> baseAlias = Optional.ofNullable(qualifiedArg.prefix())
        .filter(p -> p.size() == 1).map(p->p.get(0));

    List<Field> fields = baseTable.walkField(qualifiedArg.suffix());
    List<Relationship> rels = fields.stream().filter(f->f instanceof Relationship).map( f->(Relationship)f).collect(
        Collectors.toList());

    TableWithPK basePkTable = tableMapper.getTable(baseTable);
    String subqAlias = uniqueAliasGenerator.generate("s");
    TablePathImpl tablePath = new TablePathImpl(baseTable, baseAlias, false, rels, subqAlias);
    JoinDeclaration declaration = JoinBuilder.expandPath(tablePath, true, joinBuilderFactory);

    Preconditions.checkState(declaration.getPullupCondition().isEmpty());

    SqlNode condition = sqlNodeBuilder.createSelfEquality(basePkTable, subqAlias, baseAlias.get());

    //Choose PK if there is no field name
    String fieldName = fields.get(fields.size() - 1) instanceof Column ? fields.get(fields.size() - 1).getName().getDisplay()
        :  tableMapper.getTable(rels.get(rels.size() - 1).getToTable()).getPrimaryKeys().get(0);


    //Call gets an identifier
    SqlBasicCall newCall = new SqlBasicCall(
        function.convertToInlineAgg().get(),
        new SqlNode[]{
            new SqlIdentifier(List.of(declaration.getLastAlias(), fieldName), SqlParserPos.ZERO)
        }, call.getParserPosition());

    List<SqlNode> groups = basePkTable.getPrimaryKeys().stream()
        .map(e->new SqlIdentifier(List.of(declaration.getFirstAlias(), e), SqlParserPos.ZERO))
        .collect(Collectors.toList());

    List<SqlNode> selectList = new ArrayList<>(groups);

    //Add grouping conditions of pk
    //LEFT JOIN (SELECT sq FROM condition) x ON x.a = y.a;
    String fieldAlias = uniqueAliasGenerator.generateFieldName();
    SqlBasicCall asField = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{
        newCall,
        new SqlIdentifier(fieldAlias, SqlParserPos.ZERO)
    }, SqlParserPos.ZERO);
    selectList.add(asField);

    SqlBasicCall as = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{
        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
            new SqlNodeList(selectList, SqlParserPos.ZERO),
            declaration.getJoinTree(), null, new SqlNodeList(groups, SqlParserPos.ZERO), null, null, null, null, null, SqlNodeList.EMPTY
        ),
        new SqlIdentifier(subqAlias, SqlParserPos.ZERO)
    }, SqlParserPos.ZERO);

    SqlIdentifier identifier = new SqlIdentifier(List.of(subqAlias, fieldAlias), SqlParserPos.ZERO);
    System.out.println("Subquery: " + as + " ON "+ condition);
    System.out.println("Identifier: " + identifier);
    this.inlineAggResults.add(new InlineAggExtractResult(identifier, as, condition, scope.getNode(), qualifiedArg));
    return identifier;
  }

}
