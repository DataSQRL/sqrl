package com.datasqrl.calcite;

import com.datasqrl.calcite.schema.ScriptPlanner;
import com.datasqrl.calcite.validator.ScriptValidator;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.parse.SqrlParserImpl;
import com.datasqrl.util.DataContextImpl;
import com.datasqrl.calcite.convert.PostgresSqlConverter;
import java.util.Arrays;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.javac.JaninoCompiler;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A facade to the calcite planner
 */
@Getter
public class QueryPlanner {

  private static final AtomicInteger uniqueCompilerId = new AtomicInteger(0);
  private final RelOptCluster cluster;
  private final RelOptPlanner planner;
  private final CatalogReader catalogReader;
  private final OperatorTable operatorTable;
  private final SqrlSchema schema;
  private final ConvertletTable convertletTable;
  private final TypeFactory typeFactory;
  private final File defaultClassDir;
  private final AtomicInteger uniqueMacroInt;
  private final HintStrategyTable hintStrategyTable;
  private final RelMetadataProvider metadataProvider;

  public QueryPlanner(CatalogReader catalogReader, OperatorTable operatorTable,
      TypeFactory typeFactory, SqrlSchema schema, RelMetadataProvider metadataProvider,
      AtomicInteger uniqueMacroInt, HintStrategyTable hintStrategyTable) {
    this.catalogReader = catalogReader;
    this.operatorTable = operatorTable;
    this.schema = schema;
    this.metadataProvider = metadataProvider;
    this.uniqueMacroInt = uniqueMacroInt;
    this.hintStrategyTable = hintStrategyTable;
    this.planner = new VolcanoPlanner(null, Contexts.empty());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    RelOptUtil.registerDefaultRules(planner, true, true);
    RelOptRules.CALC_RULES.forEach(planner::addRule);
    EnumerableRules.rules().forEach(planner::addRule);

    RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);
    EnumerableRules.ENUMERABLE_RULES.forEach(planner::addRule);

    this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    this.convertletTable = new ConvertletTable();
    this.typeFactory = typeFactory;
    this.defaultClassDir = new File("build/calcite/classes");
    cluster.setMetadataProvider(this.metadataProvider);
    cluster.setHintStrategies(hintStrategyTable);
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
            .parse(sql, ErrorCollector.root());
      case FLINK:
      default:
        throw new RuntimeException("Unknown dialect");
    }
  }

  /* Plan */
  public SqlNode validate(Dialect dialect, SqlNode query, SqlValidator validator) {
    switch (dialect) {
      case SQRL:
        break;
      case CALCITE:
        return validator.validate(query);
      case FLINK:
        break;
      case POSTGRES:
        break;
    }
    return null;

  }

  public RelNode planSqrl(SqlNode query, ScriptValidator validator) {
    ScriptPlanner scriptPlanner = new ScriptPlanner(this, validator);
    return scriptPlanner.plan(query);
  }
  public RelNode plan(Dialect dialect, SqlNode query) {
    switch (dialect) {
      case SQRL:
        ScriptPlanner scriptPlanner = new ScriptPlanner(this, null);
        return scriptPlanner.plan(query);
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
        RelDecorrelator.decorrelateQuery(root.rel, relBuilder));

    return root;
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

  public Project planExpressions(List<SqlNode> nodes, Table table, String name) {
    try {
      schema.add(name, table);
      SqlValidator sqlValidator = createSqlValidator();

      SqlSelect select = new SqlSelect(SqlParserPos.ZERO,
          null,
          new SqlNodeList(nodes, SqlParserPos.ZERO),
          new SqlIdentifier(name, SqlParserPos.ZERO),
          null, null, null, null, null, null, null, SqlNodeList.EMPTY
      );
      SqlNode validated = sqlValidator.validate(select);

      SqlToRelConverter sqlToRelConverter = createSqlToRelConverter(sqlValidator);
      RelRoot root;
      root = sqlToRelConverter.convertQuery(validated, false, true);
      if (!(root.rel instanceof LogicalProject)) {
        throw new RuntimeException("Could not plan expression");
      }

      return ((LogicalProject) root.rel);
    } finally {
      schema.removeTable("@");
    }
  }

  public RexNode planExpression(SqlNode node, Table table, String name) {
    try {
      schema.add(name, table);
      SqlValidator sqlValidator = createSqlValidator();

      SqlSelect select = new SqlSelect(SqlParserPos.ZERO,
          null,
          new SqlNodeList(List.of(node), SqlParserPos.ZERO),
          new SqlIdentifier(name, SqlParserPos.ZERO),
          null, null, null, null, null, null, null, SqlNodeList.EMPTY
      );
      SqlNode validated = sqlValidator.validate(select);

      SqlToRelConverter sqlToRelConverter = createSqlToRelConverter(sqlValidator);
      RelRoot root;
      root = sqlToRelConverter.convertQuery(validated, false, true);
      if (!(root.rel instanceof LogicalProject)) {
        throw new RuntimeException("Could not plan expression");
      }

      return ((LogicalProject) root.rel).getProjects().get(0);
    } finally {
      schema.removeTable("@");
    }
  }

  /* Optimize */

  public RelNode runStage(OptimizationStage stage, RelNode relNode) {
    return RelStageRunner.runStage(stage, relNode, this.planner);
  }

  /* Convert */

  public RelNode convertRelToDialect(Dialect dialect, RelNode relNode) {
    switch (dialect) {
      case SQRL:
        break;
      case CALCITE:
        break;
      case FLINK:
        break;
      case POSTGRES:
        relNode = new DialectCallConverter(planner)
            .convert(dialect, relNode);
        return relNode;
    }

    throw new RuntimeException("Unknown dialect");
  }

  public SqlNode relToSql(Dialect dialect, RelNode relNode) {

    switch (dialect) {
      case POSTGRES:
        return new PostgresSqlConverter()
            .convert(relNode);
      case CALCITE:
        SqlNode node = new RelToSqlConverterWithHints(CalciteSqlDialect.DEFAULT).visitRoot(relNode).asStatement();
        CalciteFixes.appendSelectLists(node);
        return node;
      case SQRL:
      case FLINK:
      default:
        throw new RuntimeException("Unknown dialect");
    }
  }

  /* Compile */

  public EnumerableRel convertToEnumerableRel(RelNode relNode) {
    return (EnumerableRel) runStage(OptimizationStage.CALCITE_ENGINE, relNode);
  }

  @SneakyThrows
  protected String generateSource(String className, EnumerableRel enumerableRel,
      HashMap carryover) {
    EnumerableRelImplementor implementor = new EnumerableRelImplementor(getRexBuilder(),
        carryover/*note: must be mutable*/);
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
      HashMap<String, Object> carryover) {
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

  public Enumerator execute(RelNode relNode, DataContextImpl context) {
    String defaultName = "SqrlExecutable" + uniqueCompilerId.incrementAndGet();
    EnumerableRel enumerableRel = convertToEnumerableRel(relNode);
    HashMap<String, Object> carryover = new HashMap<>();
    ClassLoader classLoader = compile(defaultName, enumerableRel, carryover);
    context.setCarryover(carryover);
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
    return new FlinkCalciteSqlValidator(
        this.operatorTable,
        catalogReader,
        typeFactory,
        SqrlConfigurations.sqlValidatorConfig);
  }

  public SqlToRelConverter createSqlToRelConverter(SqlValidator validator) {

    return new SqlToRelConverter((relDataType, sql, c, d) -> {
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

  public SqrlRelBuilder getSqrlRelBuilder() {
    return new SqrlRelBuilder(getRelBuilder(), this.catalogReader, this);
  }

  public RexBuilder getRexBuilder() {
    return new RexBuilder(typeFactory);
  }

  public String sqlToString(Dialect dialect, SqlNode node) {
    switch (dialect) {
      case SQRL:
        break;
      case CALCITE:
        SqlWriterConfig config2 = SqrlConfigurations.sqlToString.apply(SqlPrettyWriter.config());
        SqlPrettyWriter prettyWriter = new SqlPrettyWriter(config2);
        node.unparse(prettyWriter, 0, 0);
        return prettyWriter.toSqlString().getSql();
      case FLINK:
        break;
      case POSTGRES:
        SqlWriterConfig config = SqrlConfigurations.sqlToString.apply(SqlPrettyWriter.config());
        DynamicParamSqlPrettyWriter writer = new DynamicParamSqlPrettyWriter(config);
        node.unparse(writer, 0, 0);
        return writer.toSqlString().getSql();
    }
    throw new RuntimeException("Unknown dialect");
  }

  public RelNode run(RelNode relNode, RelRule... rules) {
    return Programs.hep(Arrays.asList(rules),false, getMetadataProvider())
        .run(getPlanner(), relNode, relNode.getTraitSet(),
            List.of(), List.of());
  }

  public String relToString(Dialect dialect, RelNode relNode) {
    return sqlToString(dialect, relToSql(dialect, relNode));
  }
}
