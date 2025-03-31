package com.datasqrl.calcite;

import static com.datasqrl.parse.AstBuilder.createParserConfig;

import com.datasqrl.calcite.convert.RelToSqlNode;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.convert.SqlConverterFactory;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.calcite.convert.SqlNodeToString.SqlStrings;
import com.datasqrl.calcite.convert.SqlToStringFactory;
import com.datasqrl.calcite.schema.ExpandTableMacroRule.ExpandTableMacroConfig;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.parse.SqrlParserImpl;
import com.datasqrl.util.DataContextImpl;
import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.SqrlRexBuilder;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlSqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqrlRelDecorrelator;
import org.apache.calcite.sql2rel.SqrlSqlToRelConverter;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.javac.JaninoCompiler;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.parse.CalciteParser;

/**
 * A facade to the calcite planner
 */
@Getter
public class QueryPlanner {

  private final RelOptCluster cluster;
  private final RelOptPlanner planner;
  private final CatalogReader catalogReader;
  private final OperatorTable operatorTable;
  private final SqrlSchema schema;
  private final ConvertletTable convertletTable;
  private final File defaultClassDir;
  private final HintStrategyTable hintStrategyTable;
  private final RelMetadataProvider metadataProvider;
  private final SqrlFramework framework;

  private final CalciteParser flinkParser;
  public QueryPlanner(SqrlFramework framework) {
    this.framework = framework;
    this.catalogReader = framework.getCatalogReader();
    this.operatorTable = framework.getSqrlOperatorTable();
    this.schema = framework.getSchema();
    this.metadataProvider = framework.getRelMetadataProvider();
    this.hintStrategyTable = framework.getHintStrategyTable();
    this.planner = new VolcanoPlanner(null, Contexts.empty());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    PlannerRules.registerDefaultRules(planner);
    EnumerableRules.rules().forEach(planner::addRule);

    RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);
    EnumerableRules.ENUMERABLE_RULES.forEach(planner::addRule);

    this.cluster = RelOptCluster.create(planner, new SqrlRexBuilder(framework.getTypeFactory()));
    this.convertletTable = new ConvertletTable();
    this.defaultClassDir = new File("build/calcite/classes");
    cluster.setMetadataProvider(this.metadataProvider);
    cluster.setHintStrategies(hintStrategyTable);

    SqlParser.Config config = SqlParser.config()
        .withParserFactory(FlinkSqlParserFactories.create(FlinkSqlConformance.DEFAULT))
        .withConformance(FlinkSqlConformance.DEFAULT)
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256);
    this.flinkParser = new CalciteParser(config);
  }

  /* Parse */

  @SneakyThrows
  public SqlNode parse(Dialect dialect, String sql) {
    switch (dialect) {
      case POSTGRES:
      case CALCITE:
        return org.apache.calcite.sql.parser.SqlParser.create(sql,
                SqrlConfigurations.calciteParserConfig)
            .parseQuery();
      case SQRL:
        return new SqrlParserImpl()
            .parse(sql);
      case FLINK:
        return flinkParser.parse(sql);
      default:
        throw new RuntimeException("Unknown dialect");
    }
  }

  /* Plan */
  public RelNode plan(Dialect dialect, SqlNode query) {
    switch (dialect) {
      case SQRL:
        break;
      case CALCITE:
        return planCalcite(query);
      case FLINK:
        break;
      case POSTGRES:
        break;
    }
    throw new RuntimeException("Unknown dialect for planning");
  }

  public RelNode planCalcite(SqlNode sqlNode) {
    sqlNode = CalciteFixes.pushDownOrder(sqlNode);

    SqlValidator validator = createSqlValidator();
    return planCalcite(validator, sqlNode);
  }

  public RelNode planCalcite(SqlValidator validator, SqlNode sqlNode) {
    sqlNode = CalciteFixes.pushDownOrder(sqlNode);

    return planRoot(validator, sqlNode).project();
  }

  protected RelRoot planRoot(SqlValidator validator, SqlNode sqlNode) {
    sqlNode = validator.validate(sqlNode);

    SqlToRelConverter sqlToRelConverter = createSqlToRelConverter(validator);
    CalciteFixes.pushDownOrder(sqlNode);

    RelRoot root;
    root = sqlToRelConverter.convertQuery(sqlNode, false, true);
    final RelBuilder relBuilder = getRelBuilder();
    root = root.withRel(
        SqrlRelDecorrelator.decorrelateQuery(root.rel, relBuilder));

    return root;
  }

  /**
   * Helper function to parse a data type from a string using calcite's full
   * type definition syntax.
   */
  @SneakyThrows
  public RelDataType parseDatatype(String datatype) {
    // Must be flink types
    datatype = datatype.replaceAll("TIMESTAMP_WITH_LOCAL_TIME_ZONE",
        "TIMESTAMP_LTZ");

    // Addl type aliases for graphql
    if (datatype.equalsIgnoreCase("datetime")) {
      return this.cluster.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 3);
    } else if (datatype.equalsIgnoreCase(CustomScalars.GRAPHQL_BIGINTEGER.getName())) {
        return this.cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    }

    if (datatype.equalsIgnoreCase(CustomScalars.GRAPHQL_BIGINTEGER.getName())) {
      return this.cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    }

    // For other types, delegate to Flink planner to create Calcite types
    String create = String.format("CREATE TABLE x (col %s)", datatype);
    SqlCreateTable parse = (SqlCreateTable)parse(Dialect.FLINK, create);
    SqlDataTypeSpec typeSpec = ((SqlRegularColumn) parse.getColumnList().get(0)).getType();

    return typeSpec.deriveType(createSqlValidator());
  }

  /**
   * Plans a sql statement against a relnode
   */
  public RexNode planExpression(SqlNode node, RelDataType type) {
    return planExpression(node, type, ReservedName.SELF_IDENTIFIER.getCanonical());
  }

  public RexNode planExpression(SqlNode node, RelDataType type, String name) {
    return planExpression(node, new TemporaryViewTable(type), name);
  }

  public RexNode planExpression(SqlNode node, Table table, String name) {
    try {
      schema.add(name, table);
      SqlValidator sqlValidator = createSqlValidator();

      SqlSelect select = new SqlSelectBuilder(node.getParserPosition())
          .setSelectList(List.of(node))
          .setFrom(new SqlIdentifier(name, SqlParserPos.ZERO))
          .build();
      SqlNode validated = sqlValidator.validate(select);

      SqlToRelConverter sqlToRelConverter = createSqlToRelConverter(sqlValidator);
      RelRoot root;
      root = sqlToRelConverter.convertQuery(validated, false, true);
      if (!(root.rel instanceof LogicalProject)) {
        throw new RuntimeException("Could not plan expression");
      }

      if (!(root.rel.getInput(0) instanceof TableScan || root.rel.getInput(0) instanceof TableFunctionScan)) {
        throw new RuntimeException("Expression is not simple");
      }

      return ((LogicalProject) root.rel).getProjects().get(0);
    } finally {
      schema.removeTable(name);
    }
  }

  public RelNode planQueryOnTempTable(RelDataType tempSchema, String name, Consumer<RelBuilder> buildQuery) {
    try {
      schema.add(name, new TemporaryViewTable(tempSchema));

      RelBuilder builder = getRelBuilder();

      buildQuery.accept(builder);

      return builder.build();
    } finally {
      schema.removeTable(name);
    }
  }

  /* Optimize */

  public RelNode runStage(OptimizationStage stage, RelNode relNode) {
    return RelStageRunner.runStage(stage, relNode, this.planner);
  }

  /* Convert */

  public RelNode convertRelToDialect(Dialect dialect, RelNode relNode) {
    return new DialectCallConverter(planner)
        .convert(dialect, relNode);
  }

  public static SqlNodes relToSql(Dialect dialect, RelNode relNode) {
    RelToSqlNode relToSql = SqlConverterFactory.get(dialect);
    return relToSql.convert(relNode);
  }

  /* Compile */

  public EnumerableRel convertToEnumerableRel(RelNode relNode) {
    return (EnumerableRel) runStage(OptimizationStage.CALCITE_ENGINE, relNode);
  }

  @SneakyThrows
  protected String generateSource(String className, EnumerableRel enumerableRel,
      Map<String, Object> contextVariables) {
    EnumerableRelImplementor implementor = new EnumerableRelImplementor(getRexBuilder(),
        contextVariables/*note: must be mutable*/);
    ClassDeclaration classDeclaration = implementor.implementRoot(enumerableRel,
        EnumerableRel.Prefer.ARRAY);

    String s = Expressions.toString(classDeclaration.memberDeclarations, "\n", false);

    String source = "public class " + className + "\n"
        + "    implements " + ArrayBindable.class.getName()
        + ", " + Serializable.class.getName()
        + " {\n"
        + s + "\n"
        + "}\n";
    return source;
  }

  public ClassLoader compile(String className, EnumerableRel enumerableRel,
      Map<String, Object> carryover) {
    return compile(className, generateSource(className, enumerableRel, carryover),
        defaultClassDir.toPath());
  }

  @SneakyThrows
  protected ClassLoader compile(String className, String source, Path path) {
    final String classFileName = className + ".java";

    Files.createDirectories(path);
    JaninoCompiler compiler = new JaninoCompiler();
    compiler.getArgs().setDestdir(path.toAbsolutePath().toString());
    compiler.getArgs().setSource(source, classFileName);
    compiler.getArgs().setFullClassName(className);
    compiler.compile();

    return compiler.getClassLoader();
  }

  /* Execute */

  public ArrayBindable bindable(ClassLoader classLoader, String className) {
    try {
      @SuppressWarnings("unchecked") final Class<ArrayBindable> clazz =
          (Class<ArrayBindable>) classLoader.loadClass(className);
      final Constructor<ArrayBindable> constructor = clazz.getConstructor();
      return constructor.newInstance();
    } catch (ClassNotFoundException | InstantiationException
             | IllegalAccessException | NoSuchMethodException
             | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public Enumerator execute(String uniqueFnName, RelNode relNode, DataContextImpl context) {
    String defaultName = uniqueFnName;
    EnumerableRel enumerableRel = convertToEnumerableRel(relNode);
    Map<String, Object> variables = new HashMap<>();
    ClassLoader classLoader = compile(defaultName, enumerableRel, variables);
    context.setContextVariables(variables);
    return bindable(classLoader, defaultName)
        .bind(context).enumerator();
  }

  /**
   * Alternative to compiling
   */
  public Enumerator<Object[]> interpertable(EnumerableRel enumerableRel,
      DataContextImpl dataContext) {
    Map<String, Object> map = new HashMap<>();
    Bindable bindable = EnumerableInterpretable.toBindable(map,
        CalcitePrepare.Dummy.getSparkHandler(false), enumerableRel, EnumerableRel.Prefer.ARRAY);
    Enumerator<Object[]> enumerator = bindable.bind(dataContext)
        .enumerator();
    return enumerator;
  }

  /* Factories */
  public SqlValidator createSqlValidator() {
    return new SqrlSqlValidator(
        this.operatorTable,
        catalogReader,
        framework.getTypeFactory(),
        SqrlConfigurations.sqlValidatorConfig);
  }

  public SqlToRelConverter createSqlToRelConverter(SqlValidator validator) {

    return new SqrlSqlToRelConverter((relDataType, sql, c, d) -> {
      SqlValidator validator1 = createSqlValidator();
      SqlNode node = parse(Dialect.CALCITE, sql);
      validator1.validate(node);

      return planRoot(validator1, node);
    },
        validator,
        catalogReader, getCluster(),
        convertletTable,
        SqrlConfigurations.sqlToRelConverterConfig
            .withHintStrategyTable(this.hintStrategyTable).withTrimUnusedFields(false));
  }

  public RelBuilder getRelBuilder() {
    return new RelBuilder(null, this.cluster, this.catalogReader){};
  }

  public RexBuilder getRexBuilder() {
    return new RexBuilder(catalogReader.getTypeFactory());
  }

  public static SqlStrings sqlToString(Dialect dialect, SqlNodes node) {
    SqlNodeToString sqlToString = SqlToStringFactory.get(dialect);
    return sqlToString.convert(node);
  }

  public RelNode run(RelNode relNode, RelRule... rules) {
    return Programs.hep(Arrays.asList(rules),false, getMetadataProvider())
        .run(getPlanner(), relNode, relNode.getTraitSet(),
            List.of(), List.of());
  }

  public static SqlStrings relToString(Dialect dialect, RelNode relNode) {
    return sqlToString(dialect, relToSql(dialect, relNode));
  }

  public RelNode expandMacros(RelNode relNode) {
    //Before macro expansion, clean up the rel
    Config expandTableMacroConfig = ExpandTableMacroConfig.DEFAULT;
    relNode = run(relNode,
        SubQueryRemoveRule.Config.PROJECT.toRule(),
        SubQueryRemoveRule.Config.FILTER.toRule(),
        SubQueryRemoveRule.Config.JOIN.toRule(),
        (RelRule) expandTableMacroConfig.toRule());

    //Convert lateral joins
    relNode = SqrlRelDecorrelator.decorrelateQuery(relNode, getRelBuilder());
    relNode = run(relNode, CoreRules.FILTER_INTO_JOIN);
    return relNode;
  }

  @SneakyThrows
  public SqlNode parseCall(String expression) {
    SqlNode sqlNode = SqlParser.create(expression, createParserConfig())
        .parseExpression();
    return sqlNode;
  }
}
