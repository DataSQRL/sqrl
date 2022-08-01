package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.environment.ImportManager.TableImport;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.*;
import ai.datasqrl.plan.local.transpile.TranspileOptions;
import ai.datasqrl.plan.calcite.SqrlConformance;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import ai.datasqrl.plan.calcite.table.CalciteTableFactory;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.plan.local.generate.FieldNames;
import ai.datasqrl.plan.local.transpile.*;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.util.data.C360;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static ai.datasqrl.plan.calcite.util.SqlNodeUtil.and;
import static org.junit.jupiter.api.Assertions.fail;

class CalciteParserTest extends AbstractSQRLIT {

  CalciteSchema schema;
  ErrorCollector errorCollector;
  ImportManager importManager;
  UniqueAliasGeneratorImpl uniqueAliasGenerator;
  JoinDeclarationContainerImpl joinDecs;
  SqlNodeBuilder sqlNodeBuilder;
  TableMapperImpl tableMapper;
  private CalciteSchema shredSchema;
  SQRLTable orders;

  @BeforeEach
  public void each() {
    errorCollector = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.INSTANCE;

    example.registerSource(env);

    importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    Assertions.assertTrue(importManager.registerUserSchema(bundle.getMainScript().getSchema(),
        ErrorCollector.root()));

    TableImport tblImport = importManager.importTable(Name.system("ecommerce-data"),
        Name.system("Orders"),
        SchemaAdjustmentSettings.DEFAULT, errorCollector);

    CalciteTableFactory tableFactory = new CalciteTableFactory(new SqrlTypeFactory(new SqrlTypeSystem()));
    ScriptTableDefinition importedTable = tableFactory.importTable((SourceTableImport)tblImport, Optional.empty(),
            new PlannerFactory(CalciteSchema.createRootSchema(false, false).plus())
                    .createPlanner().getRelBuilder());
    tableMapper = new TableMapperImpl((Map)importedTable.getShredTableMap());
    uniqueAliasGenerator = new UniqueAliasGeneratorImpl();
    joinDecs = new JoinDeclarationContainerImpl();
    sqlNodeBuilder = new SqlNodeBuilder();

    importedTable.getShredTableMap().keySet().stream().flatMap(t->t.getAllRelationships())
        .forEach(r->{
          SqlJoinDeclaration d = createParentChildJoinDeclaration(r, tableMapper, uniqueAliasGenerator);
          joinDecs.add(r, d);
        });

    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    schema.add(importedTable.getTable().getName().getDisplay(),
        (Table)importedTable.getTable());
    this.schema = schema;

    CalciteSchema shredSchema = CalciteSchema.createRootSchema(true);
    importedTable.getShredTableMap().values().stream().forEach(vt -> shredSchema.add(vt.getNameId(),vt));

    this.shredSchema = shredSchema;
    orders = (SQRLTable) schema.getTable("Orders", false).getTable();

  }

  @Test
  public void test1() throws SqlParseException {
    //select * does not include relationships
    validate("SELECT * FROM Orders", schema);
  }

  @Test
  public void test2() throws SqlParseException {
    //can walk path
    validate("SELECT * FROM Orders.entries", schema);
  }

  @Test
  public void test3() throws SqlParseException {
    validate("SELECT * FROM Orders.entries.parent", schema);
  }

  @Test
  public void test4() throws SqlParseException {
    //TODO: This produces a really bad plan
    validate("SELECT x.parent.* FROM Orders.entries AS x", schema);
  }
  @Test
  public void test5() throws SqlParseException {
    validate("SELECT sum(sumx(parent.id)) AS x FROM Orders.entries", schema);
  }

  @Test
  public void test6() throws SqlParseException {
    validate("SELECT * FROM _ JOIN Orders ON Orders.customerid = _.customerid", schema,
        Optional.of(orders), new FieldNames());
  }

  @Test
  public void test7() throws SqlParseException {
    //can inline agg
    validate("SELECT sumx(parent.id) AS x "
        + "FROM Orders.entries "
        + "ORDER BY sumx(parent.id)", schema);
  }

  @Test
  public void testSum() throws SqlParseException {
    //can inline agg
    validate("SELECT discount + discount "
        + "FROM Orders.entries e", schema);
  }


  @Test
  public void test8() throws SqlParseException {

//    //self join
//    validate("SELECT * FROM _", schema, Optional.of(ordersT));
//    validate("SELECT parent.* FROM _.entries", schema, Optional.of(ordersT));

  }

  @Test
  public void test9() throws SqlParseException {
//    validate("SELECT e.* "
//        + "FROM _.entries e", schema, Optional.of(ordersT));
  }

  @Test
  public void testSelfResolve() throws SqlParseException {
//    try {
//      validate("SELECT customerid "
//          + "FROM _.entries", schema, Optional.of(ordersT));
//      fail();/
//    } catch (Exception e) {
//
//    }
  }

  @Test
  public void testSelfResolve2() throws SqlParseException {
//    validate("SELECT _.customerid "
//          + "FROM _.entries", schema, Optional.of(ordersT));
  }

  //TODO
//  @Test
//  public void testOnHasPath() throws SqlParseException {
//    validate("SELECT o.* "
//        + "FROM Orders.entries e "
//        //Can only call parent in ON clause
//        + "JOIN Orders o ON e.parent._uuid = o._uuid", schema);
//  }

  @Test
  public void testx() throws SqlParseException {
    validate("SELECT o.* "
        + "FROM Orders.entries e "
        //Can only call parent in ON clause
        + "JOIN Orders o ON true", schema);
  }

  // b. o.entries
  @Test
  public void test10() throws SqlParseException {
        validate("SELECT o.* "
        + "FROM Orders.entries e "
        + "JOIN e.parent o", schema);
  }

  @Test
  public void testInvalid10() throws SqlParseException {
    try {
      validate("SELECT SUM(O.entries) "
          + "FROM Orders.entries E "
          + "JOIN e.parent o", schema);
      fail();
    } catch (Exception e) {

    }
  }

  @Test
  public void test11() throws SqlParseException {
    validate("SELECT sum(count(o.entries)) "
        + "FROM Orders o ", schema);
  }

  @Test
  public void test12() throws SqlParseException {
    validate("SELECT sum(count(o.entries)) "
        + "FROM Orders o ", schema);
  }


  @Test
  public void identifierExpand() throws SqlParseException {
    //Expands an Identifier w/ or w/o a scalar field at the end into a set of joins (w/ a trailing condition)

    Pair<SqlNode, SqrlValidatorImpl> pair = validate(
        "SELECT p.id, sum(sum(p.entries.quantity)) "
            + "FROM Orders.entries.parent p "
            + "LEFT JOIN p.entries e GROUP BY p.id", schema);
    SqlNode node = pair.getLeft();
    SqrlValidatorImpl validator = pair.getRight();

    System.out.println();
  }

  @Test
  public void letsdoit() throws SqlParseException {

//    Pair<SqlNode, SqlValidatorImpl> pair2 = validate( //p.entries is same name?
//        "SELECT entries.discount, sum(sum(p.entries.quantity)) "
//            + "FROM Orders.entries.parent p "
//            + "LEFT JOIN p.entries  GROUP BY entries.discount", schema);

//    Pair<SqlNode, SqlValidatorImpl> pair3 = validate( //p.entries is same name?
//        "SELECT p.id, sum(sum(p.entries.quantity)) "
//            + "FROM Orders.entries.parent p "
//            + "LEFT JOIN p.entries "
//            + "LEFT JOIN p.entries.parent.entries f GROUP BY p.id", schema);
//
    //TODO: to-one should remain aggregating rather than breakout
//    Pair<SqlNode, SqlValidatorImpl> pair3 = validate(
//        "SELECT e.parent.id, sum(e.parent.id) s "
//            + "FROM Orders.entries e "
//            + "GROUP BY 1 ", schema);
    SQRLTable SQRLTable = (SQRLTable) schema.getTable("Orders", false).getTable();

//
//    //TODO: don't duplicate
    Pair<SqlNode, SqrlValidatorImpl> pair3 = validate(
        "SELECT p.id, sum(p.entries.quantity) s, sum(sum(p.entries.quantity)) "
            + "FROM _ JOIN _.entries.parent p "
            + "LEFT JOIN p.entries e "
            + "GROUP BY 1, s "
            + "ORDER BY p.id, sum(p.entries.quantity)", schema, Optional.of(SQRLTable), new FieldNames());



//    Pair<SqlNode, SqlValidatorImpl> pair3 = validate(
//        "SELECT p.id, e.parent.id "
//            + "FROM Orders.entries.parent p "
//            + "LEFT JOIN p.entries e ", schema);

    FieldNames names = new FieldNames();
    Field discount =
        ((Relationship) SQRLTable.getField(Name.system("entries")).get()).getToTable().getField(Name.system("discount")).get();
    names.put(discount, "discount1");

//    validate(
//        "SELECT _._uuid x "
//            + "FROM _ JOIN _.entries e "
//            + "ORDER BY _._uuid", schema, Optional.of(SQRLTable), names);

  }

  protected SqlJoinDeclaration createParentChildJoinDeclaration(Relationship rel, TableMapperImpl tableMapper,
      UniqueAliasGeneratorImpl uniqueAliasGenerator) {
    TableWithPK pk = tableMapper.getTable(rel.getToTable());
    String alias = uniqueAliasGenerator.generate(pk);
    return new SqlJoinDeclarationImpl(
        Optional.of(createParentChildCondition(rel, alias, tableMapper)),
        createTableRef(rel.getToTable(), alias, tableMapper), "_", alias);
  }

  protected SqlNode createTableRef(SQRLTable table, String alias, TableMapperImpl tableMapper) {
    return new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
        new SqlIdentifier(tableMapper.getTable(table).getNameId(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
        new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }

  protected SqlNode createParentChildCondition(Relationship rel, String alias, TableMapperImpl tableMapper) {
    TableWithPK lhs =
        rel.getJoinType().equals(Relationship.JoinType.PARENT) ? tableMapper.getTable(rel.getFromTable())
            : tableMapper.getTable(rel.getToTable());
    TableWithPK rhs =
        rel.getJoinType().equals(Relationship.JoinType.PARENT) ? tableMapper.getTable(rel.getToTable())
            : tableMapper.getTable(rel.getFromTable());

    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < lhs.getPrimaryKeys().size(); i++) {
      String lpk = lhs.getPrimaryKeys().get(i);
      String rpk = rhs.getPrimaryKeys().get(i);
      conditions.add(new SqlBasicCall(SqrlOperatorTable.EQUALS,
          new SqlNode[]{new SqlIdentifier(List.of("_", lpk), SqlParserPos.ZERO),
              new SqlIdentifier(List.of(alias, rpk), SqlParserPos.ZERO)}, SqlParserPos.ZERO));
    }

    return and(conditions);
  }


  @Test
  public void expand() {
//    TableMapper tableMapper = table -> () -> List.of("_uuid");
//    AtomicInteger i = new AtomicInteger();
//    UniqueAliasGenerator aliasGenerator = table -> "a" + i.incrementAndGet() + "";
//    JoinDeclarationContainer declarationContainer = rel -> new JoinDeclarationImpl(
//        Optional.of(SqlLiteral.createBoolean(true, SqlParserPos.ZERO)),
//        new SqlTableRef(SqlParserPos.ZERO, new SqlIdentifier("entries", SqlParserPos.ZERO), SqlNodeList.EMPTY),
//        "e");

//
//    //o.entries
//    TablePath tablePath = new TablePath() {
//      @Override
//      public SQRLTable getBaseTable() {
//        return new SQRLTable(NamePath.of("entries"));
//      }
//
//      @Override
//      public Optional<String> getBaseAlias() {
//        return Optional.of("o");
//      }
//
//      @Override
//      public boolean isRelative() {
//        return false;
//      }
//
//      @Override
//      public int size() {
//        return 2;
//      }
//
//      @Override
//      public Relationship getRelationship(int i) {
//        return mock(Relationship.class);
//      }
//
//      @Override
//      public String getAlias() {
//        return "e1";
//      }
//    };
    //forceincldue base determiens if we're ina subquery
//    JoinDeclaration d = expandPath(tablePath, true/*true*/, () -> new JoinBuilder(aliasGenerator,
//        declarationContainer, tableMapper, sqlBuilder));

//    System.out.println(SqlNodeUtil.printJoin(d.getJoinTree()));
//    System.out.println(d.getLastAlias());
//    System.out.println(d.getPullupCondition());
  }


  public Pair<SqlNode, SqrlValidatorImpl> validate(String query, CalciteSchema schema) throws SqlParseException {
    return validate(query, schema, Optional.empty(), new FieldNames());
  }

  private Pair<SqlNode, SqrlValidatorImpl> validate(String query, CalciteSchema schema, Optional<SQRLTable> ctx, FieldNames names)
      throws SqlParseException {
    SqlNode node = SqlParser.create(query, SqlParser.config().withCaseSensitive(false)
        .withUnquotedCasing(Casing.UNCHANGED)).parseQuery();
    Properties p = new Properties();
    p.put(CalciteConnectionProperty.CASE_SENSITIVE.name(), false);
    SqrlValidatorImpl validator = new SqrlValidatorImpl(
        SqlStdOperatorTable.instance(),
        new CalciteCatalogReader(schema, List.of(), new SqrlTypeFactory(new SqrlTypeSystem()),
            new CalciteConnectionConfigImpl(p).set(CalciteConnectionProperty.CASE_SENSITIVE,
                "false")),
        new SqrlTypeFactory(new SqrlTypeSystem()),
        SqlValidator.Config.DEFAULT.withSqlConformance(SqrlConformance.INSTANCE)
            .withCallRewrite(true)
            .withIdentifierExpansion(false)
            .withColumnReferenceExpansion(false)
            .withTypeCoercionEnabled(false)
            .withLenientOperatorLookup(true));

    //I don't want to add it to the schema b/c it should only be valid on the root query
//    ctx.map(t -> schema.add("_", (Table) t));
    //TODO: Explicit or non-explicit self table
    validator.setContext(ctx);
    System.out.println(node);
    validator.validate(node);
//    ctx.map(t -> schema.removeTable("_"));
    System.out.println(node);

    SqlSelect select = node instanceof SqlSelect ? (SqlSelect)node : (SqlSelect) ((SqlOrderBy)node).query;
    SqlValidatorScope scope = validator.getSelectScope(select);

    Transpile transpile = new Transpile(
        validator,tableMapper, uniqueAliasGenerator, joinDecs,
        sqlNodeBuilder, () -> new JoinBuilderImpl(uniqueAliasGenerator, joinDecs, tableMapper), names,
        TranspileOptions.builder().build());

    transpile.rewriteQuery(select, scope);
    System.out.println("\nRewritten: " + select);


    SqlValidator validator2 = SqlValidatorUtil.newValidator(
        SqlStdOperatorTable.instance(),
        new CalciteCatalogReader(shredSchema, List.of(), new SqrlTypeFactory(new SqrlTypeSystem()),
            new CalciteConnectionConfigImpl(p).set(CalciteConnectionProperty.CASE_SENSITIVE,
                "false")),
        new SqrlTypeFactory(new SqrlTypeSystem()),
        SqlValidator.Config.DEFAULT.withSqlConformance(SqrlConformance.INSTANCE)
            .withCallRewrite(true)
            .withIdentifierExpansion(false)
            .withColumnReferenceExpansion(false)
            .withTypeCoercionEnabled(false)
            .withLenientOperatorLookup(true));
    validator2.validate(select);

    return Pair.of(select, validator);
  }
}