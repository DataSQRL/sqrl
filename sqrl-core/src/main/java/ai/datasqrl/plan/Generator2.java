//package ai.datasqrl.plan.local.generate;
//
//import static ai.datasqrl.plan.local.generate.node.util.SqlNodeUtil.and;
//
//import ai.datasqrl.SqrlSchema;
//import ai.datasqrl.config.error.ErrorCollector;
//import ai.datasqrl.environment.ImportManager;
//import ai.datasqrl.environment.ImportManager.SourceTableImport;
//import ai.datasqrl.environment.ImportManager.TableImport;
//import ai.datasqrl.parse.Check;
//import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
//import ai.datasqrl.parse.tree.DistinctAssignment;
//import ai.datasqrl.parse.tree.ImportDefinition;
//import ai.datasqrl.parse.tree.JoinAssignment;
//import ai.datasqrl.parse.tree.Node;
//import ai.datasqrl.parse.tree.ScriptNode;
//import ai.datasqrl.parse.tree.SqrlStatement;
//import ai.datasqrl.parse.tree.name.Name;
//import ai.datasqrl.parse.tree.name.NamePath;
//import ai.datasqrl.parse.tree.name.ReservedName;
//import ai.datasqrl.plan.calcite.Planner;
//import ai.datasqrl.plan.calcite.SqrlOperatorTable;
//import ai.datasqrl.plan.calcite.TranspilerFactory;
//import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
//import ai.datasqrl.plan.calcite.sqrl.table.CalciteTableFactory;
//import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;
//import ai.datasqrl.plan.calcite.sqrl.table.VirtualSqrlTable;
//import ai.datasqrl.plan.local.Errors;
//import ai.datasqrl.plan.local.ImportedTable;
//import ai.datasqrl.plan.local.analyze.VariableFactory;
//import ai.datasqrl.plan.local.generate.QueryGenerator.FieldNames;
//import ai.datasqrl.plan.local.generate.QueryGenerator.Scope;
//import ai.datasqrl.schema.Relationship;
//import ai.datasqrl.schema.ScriptTable;
//import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
//import com.google.common.base.Preconditions;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import lombok.SneakyThrows;
//import lombok.Value;
//import org.apache.calcite.avatica.util.Casing;
//import org.apache.calcite.jdbc.CalciteSchema;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.RelRoot;
//import org.apache.calcite.schema.SchemaPlus;
//import org.apache.calcite.schema.Table;
//import org.apache.calcite.sql.JoinBuilder;
//import org.apache.calcite.sql.JoinDeclaration;
//import org.apache.calcite.sql.JoinDeclarationContainerImpl;
//import org.apache.calcite.sql.JoinDeclarationImpl;
//import org.apache.calcite.sql.SqlBasicCall;
//import org.apache.calcite.sql.SqlIdentifier;
//import org.apache.calcite.sql.SqlJoin;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.SqlNodeBuilderImpl;
//import org.apache.calcite.sql.SqlNodeList;
//import org.apache.calcite.sql.SqlOrderBy;
//import org.apache.calcite.sql.SqlSelect;
//import org.apache.calcite.sql.SqlTableRef;
//import org.apache.calcite.sql.TableMapperImpl;
//import org.apache.calcite.sql.Transpile;
//import org.apache.calcite.sql.UniqueAliasGeneratorImpl;
//import org.apache.calcite.sql.parser.SqlParser;
//import org.apache.calcite.sql.parser.SqlParserPos;
//import org.apache.calcite.sql.validate.SqlValidator;
//import org.apache.calcite.sql.validate.SqlValidatorScope;
//import org.apache.calcite.sql.validate.SqrlValidatorImpl;
//
//public class Generator2 extends DefaultTraversalVisitor<SqlNode, Scope> {
//
//  protected final Map<ScriptTable, AbstractSqrlTable> tableMap = new HashMap<>();
//  final FieldNames fieldNames = new FieldNames();
//  private final CalciteSchema transpileSchema;
//  private final SchemaPlus shredSchema;
//  ErrorCollector errors;
//  CalciteTableFactory tableFactory;
//  SchemaAdjustmentSettings schemaSettings;
//  Planner planner;
//  ImportManager importManager;
//  UniqueAliasGeneratorImpl uniqueAliasGenerator;
//  JoinDeclarationContainerImpl joinDecs;
//  SqlNodeBuilderImpl sqlNodeBuilder;
//  TableMapperImpl tableMapper;
//  VariableFactory variableFactory;
//
//
//  public Generator2(CalciteTableFactory tableFactory, SchemaAdjustmentSettings schemaSettings,
//      Planner planner, ImportManager importManager, UniqueAliasGeneratorImpl uniqueAliasGenerator,
//      JoinDeclarationContainerImpl joinDecs, SqlNodeBuilderImpl sqlNodeBuilder,
//      TableMapperImpl tableMapper, ErrorCollector errors, VariableFactory variableFactory) {
//    this.tableFactory = tableFactory;
//    this.schemaSettings = schemaSettings;
//    this.planner = planner;
//    this.importManager = importManager;
//    this.uniqueAliasGenerator = uniqueAliasGenerator;
//    this.joinDecs = joinDecs;
//    this.sqlNodeBuilder = sqlNodeBuilder;
//    this.tableMapper = tableMapper;
//    this.errors = errors;
//    this.variableFactory = variableFactory;
//    this.shredSchema = planner.getDefaultSchema();
//    this.transpileSchema = CalciteSchema.createRootSchema(true);
//  }
//
//  public void generate(ScriptNode scriptNode) {
//    for (Node statement : scriptNode.getStatements()) {
//      statement.accept(this, null);
//    }
//  }
//
//  public SqlNode generate(SqrlStatement statement) {
//    return statement.accept(this, null);
//  }
//
//  /**
//   * In order to expose a hierarchical table in Calcite, we need to register the source dataset with
//   * the full calcite schema and a table for each nested record, which we can then query.
//   * <p>
//   * To do this, we use a process called table shredding. This involves removing the nested records
//   * from the source dataset, and then registering the resulting table with the calcite schema.
//   * <p>
//   * We can then expand this into a full logical plan using the
//   * {@link ai.datasqrl.plan.calcite.sqrl.rules.SqrlExpansionRelRule}
//   */
//  @Override
//  public SqlNode visitImportDefinition(ImportDefinition node, Scope context) {
//    if (node.getNamePath().size() != 2) {
//      throw new RuntimeException(
//          String.format("Invalid import identifier: %s", node.getNamePath()));
//    }
//
//    //Check if this imports all or a single table
//    Name sourceDataset = node.getNamePath().get(0);
//    Name sourceTable = node.getNamePath().get(1);
//
//    List<TableImport> importTables;
//    Optional<Name> nameAlias = node.getAliasName();
//    List<ImportedTable> dt = new ArrayList<>();
//    if (sourceTable.equals(ReservedName.ALL)) { //import all tables from dataset
//      importTables = importManager.importAllTables(sourceDataset, schemaSettings, errors);
//      nameAlias = Optional.empty();
//      throw new RuntimeException("TBD");
//    } else { //importing a single table
//      //todo: Check if table exists
//      TableImport tblImport = importManager.importTable(sourceDataset, sourceTable,
//          schemaSettings, errors);
//      if (!tblImport.isSource()) {
//        throw new RuntimeException("TBD");
//      }
//      SourceTableImport sourceTableImport = (SourceTableImport) tblImport;
//
//      ImportedTable importedTable = tableFactory.importTable(sourceTableImport, nameAlias);
//      dt.add(importedTable);
//    }
//
//    for (ImportedTable d : dt) {
//      //Add all Calcite tables to the schema
////      this.tables.put(d.getImpTable().getNameId(), d.getImpTable());
//      d.getShredTableMap().values().stream().forEach(vt -> shredSchema.add(vt.getNameId(), vt));
//
//      //Update table mapping from SQRL table to Calcite table...
////      this.tableMap.putAll(d.getShredTableMap());
//      //and also map all fields
//      this.tableMapper.getTableMap().putAll(d.getShredTableMap());
//
//      this.fieldNames.putAll(d.getFieldNameMap());
//
//      d.getShredTableMap().keySet().stream().flatMap(t -> t.getAllRelationships())
//          .forEach(r -> {
//            JoinDeclaration dec = createParentChildJoinDeclaration(r, tableMapper,
//                uniqueAliasGenerator);
//            joinDecs.add(r, dec);
//          });
//
//      SqrlSchema datasets = new SqrlSchema();
//      transpileSchema.add(d.getName().getDisplay(), datasets);
//      transpileSchema.add(d.getTable().getName().getDisplay(),
//          (Table) d.getTable());
//    }
//
//    return null;
//  }
//
//  @Override
//  public SqlNode visitDistinctAssignment(DistinctAssignment node, Scope context) {
//    //todo:
////    SqlNode sqlNode = generateDistinctQuery(node);
////    //Field names are the same...
////    ScriptTable table = analysis.getProducedTable().get(node);
////
////    RelNode relNode = plan(sqlNode);
////    for (int i = 0; i < analysis.getProducedFieldList().get(node).size(); i++) {
////      this.fieldNames.put(analysis.getProducedFieldList().get(node).get(i),
////          relNode.getRowType().getFieldList().get(i).getName());
////    }
////
////    int numPKs = node.getPartitionKeyNodes().size();
////    QueryCalciteTable queryTable = new QueryCalciteTable(relNode);
////    this.tables.put(queryTable.getNameId(), queryTable);
////    this.tableMap.put(table, queryTable);
////    return sqlNode;
//    return null;
//  }
//
//  @SneakyThrows
//  @Override
//  public SqlNode visitJoinAssignment(JoinAssignment node, Scope context) {
////    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_TABLE);
//    Check.state(node.getNamePath().size() > 1, node, Errors.JOIN_ON_ROOT);
//    Optional<ScriptTable> table = getContext(node.getNamePath().popLast());
//    Preconditions.checkState(table.isPresent());
//    String query = node.getSqlQuery();
//    TranspiledResult result = transpile(query, table);
//
//    SqlBasicCall tRight = (SqlBasicCall) getRightDeepTable(result.getSqlNode());
//    VirtualSqrlTable vt = result.getSqlValidator().getNamespace(tRight).getTable()
//        .unwrap(VirtualSqrlTable.class);
//    ScriptTable toTable = getScriptTable(vt);
//
//    Relationship relationship = variableFactory.addJoinDeclaration(node.getNamePath(), table.get(),
//        toTable, node.getJoinDeclaration().getLimit());
//
//    //todo: assert non-shadowed
//    this.fieldNames.put(relationship, relationship.getName().getCanonical());
//    SqlNode join = unwrapSelect(result.getSqlNode()).getFrom();
//
//    SqlJoin join1 = (SqlJoin) join;
//
//    this.joinDecs.add(relationship, new JoinDeclarationImpl(
//        Optional.of(join1.getCondition()),
//        join1.getRight(),
//        "_",
//        ((SqlIdentifier) tRight.getOperandList().get(1)).names.get(0)));
//    return null;
//  }
//
//  private ScriptTable getScriptTable(VirtualSqrlTable vt) {
//    for (Map.Entry<ScriptTable, VirtualSqrlTable> t : this.tableMapper.getTableMap().entrySet()) {
//      if (t.getValue().equals(vt)) {
//        return t.getKey();
//      }
//    }
//    return null;
//  }
//
//  private SqlNode getRightDeepTable(SqlNode node) {
//    if (node instanceof SqlSelect) {
//      return getRightDeepTable(((SqlSelect) node).getFrom());
//    } else if (node instanceof SqlOrderBy) {
//      return getRightDeepTable(((SqlOrderBy) node).query);
//    } else if (node instanceof SqlJoin) {
//      return getRightDeepTable(((SqlJoin) node).getRight());
//    } else {
//      return node;
//    }
//  }
//
//  //todo: fix for union etc
//  private SqlSelect unwrapSelect(SqlNode sqlNode) {
//    if (sqlNode instanceof SqlOrderBy) {
//      return (SqlSelect) ((SqlOrderBy) sqlNode).query;
//    }
//    return (SqlSelect) sqlNode;
//  }
//
//  private ScriptTable getContextOrThrow(NamePath namePath) {
//    return getContext(namePath).orElseThrow(() -> new RuntimeException(""));
//  }
//
//  private Optional<ScriptTable> getContext(NamePath namePath) {
//    if (namePath.size() == 0) {
//      return Optional.empty();
//    }
//    ScriptTable table = (ScriptTable) transpileSchema.getTable(namePath.get(0).getDisplay(), false)
//        .getTable();
//
//    return table.walkTable(namePath.popFirst());
//  }
//
//  @Value
//  class SqrlValidateResult {
//
//    SqlNode validated;
//    SqrlValidatorImpl validator;
//  }
//
//  @SneakyThrows
//  private TranspiledResult transpile(String query, Optional<ScriptTable> context) {
//    System.out.println(query);
//    SqlNode node = SqlParser.create(query, SqlParser.config().withCaseSensitive(false)
//        .withUnquotedCasing(Casing.UNCHANGED)).parseQuery();
//
//    SqrlValidateResult result = validate(node, context);
//    TranspiledResult transpiledResult = transpile(result.getValidated(), result.getValidator());
//    return transpiledResult;
//  }
//
//  private TranspiledResult transpile(SqlNode node, SqrlValidatorImpl validator) {
//    SqlSelect select =
//        node instanceof SqlSelect ? (SqlSelect) node : (SqlSelect) ((SqlOrderBy) node).query;
//    SqlValidatorScope scope = validator.getSelectScope(select);
//
//    Transpile transpile = new Transpile(
//        validator, tableMapper, uniqueAliasGenerator, joinDecs,
//        sqlNodeBuilder,
//        () -> new JoinBuilder(uniqueAliasGenerator, joinDecs, tableMapper, sqlNodeBuilder),
//        fieldNames);
//
//    transpile.rewriteQuery(select, scope);
//
//    System.out.println("Rewritten: " + select);
//    SqlValidator sqlValidator = TranspilerFactory.createSqlValidator(
//        shredSchema.unwrap(CalciteSchema.class));
//    SqlNode validated = sqlValidator.validate(select);
//
//    return new TranspiledResult(select, validator, node,
//        sqlValidator, plan(validated, sqlValidator));
//  }
//
//  private SqrlValidateResult validate(SqlNode node, Optional<ScriptTable> context) {
//    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(transpileSchema);
//    sqrlValidator.setContext(context);
//    sqrlValidator.validate(node);
//    return new SqrlValidateResult(node, sqrlValidator);
//  }
//
//  @Value
//  class TranspiledResult {
//
//    SqlNode sqrlNode;
//    SqrlValidatorImpl sqrlValidator;
//    SqlNode sqlNode;
//    SqlValidator sqlValidator;
//    RelNode relNode;
//  }
//
//  protected JoinDeclaration createParentChildJoinDeclaration(Relationship rel, TableMapperImpl tableMapper,
//      UniqueAliasGeneratorImpl uniqueAliasGenerator) {
//    TableWithPK pk = tableMapper.getTable(rel.getToTable());
//    String alias = uniqueAliasGenerator.generate(pk);
//    return new JoinDeclarationImpl(
//        Optional.of(createParentChildCondition(rel, alias, tableMapper)),
//        createTableRef(rel.getToTable(), alias, tableMapper), "_", alias);
//  }
//
//  protected SqlNode createTableRef(ScriptTable table, String alias, TableMapperImpl tableMapper) {
//    return new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
//        new SqlIdentifier(tableMapper.getTable(table).getNameId(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
//        new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
//  }
//
//  protected SqlNode createParentChildCondition(Relationship rel, String alias, TableMapperImpl tableMapper) {
//    TableWithPK lhs =
//        rel.getJoinType().equals(Relationship.JoinType.PARENT) ? tableMapper.getTable(rel.getFromTable())
//            : tableMapper.getTable(rel.getToTable());
//    TableWithPK rhs =
//        rel.getJoinType().equals(Relationship.JoinType.PARENT) ? tableMapper.getTable(rel.getToTable())
//            : tableMapper.getTable(rel.getFromTable());
//
//    List<SqlNode> conditions = new ArrayList<>();
//    for (int i = 0; i < lhs.getPrimaryKeys().size(); i++) {
//      String lpk = lhs.getPrimaryKeys().get(i);
//      String rpk = rhs.getPrimaryKeys().get(i);
//      conditions.add(new SqlBasicCall(SqrlOperatorTable.EQUALS,
//          new SqlNode[]{new SqlIdentifier(List.of("_", lpk), SqlParserPos.ZERO),
//              new SqlIdentifier(List.of(alias, rpk), SqlParserPos.ZERO)}, SqlParserPos.ZERO));
//    }
//
//    return and(conditions);
//  }
//  @SneakyThrows
//  private RelNode plan(SqlNode sqlNode, SqlValidator sqlValidator) {
//    System.out.println(sqlNode);
//    planner.refresh();
//    planner.setValidator(sqlNode, sqlValidator);
//
//    RelRoot root = planner.rel(sqlNode);
//    RelNode relNode = root.rel;
//
//    return relNode;
//  }
//}