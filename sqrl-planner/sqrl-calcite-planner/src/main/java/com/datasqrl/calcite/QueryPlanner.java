package com.datasqrl.calcite;

import com.datasqrl.util.DataContextImpl;
import com.datasqrl.calcite.convert.PostgresSqlConverter;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A facade to the calcite planner
 */
@Getter
public class QueryPlanner {

  private final RelOptCluster cluster;
  private final RelOptPlanner planner;
  private final CatalogReader catalogReader;
  private final OperatorTable operatorTable;
  private final ConvertletTable convertletTable;
  private final TypeFactory typeFactory;
  private final File defaultClassDir;

  private static final AtomicInteger uniqueCompilerId = new AtomicInteger(0);

  public QueryPlanner(CatalogReader catalogReader, OperatorTable operatorTable, TypeFactory typeFactory) {
    this.catalogReader = catalogReader;
    this.operatorTable = operatorTable;
    this.planner = new VolcanoPlanner(null, Contexts.empty());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    RelOptUtil.registerDefaultRules(planner,true, true);
    RelOptRules.CALC_RULES.forEach(planner::addRule);
    RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);
    EnumerableRules.ENUMERABLE_RULES.forEach(planner::addRule);

    this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    this.convertletTable = new ConvertletTable();
    this.typeFactory = typeFactory;
    this.defaultClassDir = new File("build/calcite/classes");
    /// STOPSHIP: 8/6/23 todo
//    cluster.setMetadataProvider(new SqrlRelMetadataProvider());
//    cluster.setMetadataQuerySupplier(SqrlRelMetadataQuery::new);
//    cluster.setHintStrategies(SqrlHintStrategyTable.getHintStrategyTable());
  }

  /* Parse */

  @SneakyThrows
  public SqlNode parse(Dialect dialect, String sql) {
    switch (dialect) {
      case POSTGRES:
      case CALCITE:
        return org.apache.calcite.sql.parser.SqlParser.create(sql)
            .parseQuery();
      case SQRL: return SqlParser.create(sql,
                SqrlConfigurations.sqrlParserConfig)
            .parseQuery();
      case FLINK:
      default:
        throw new RuntimeException("Unknown dialect");
    }
  }

  /* Plan */

  public RelNode plan(SqlNode sqlNode) {
    //odd behavior by calcite parser, im doing something wrong
    if (sqlNode instanceof SqlOrderBy) {
      SqlOrderBy order = (SqlOrderBy)sqlNode;
      SqlSelect select = ((SqlSelect) order.query);
      select
          .setOrderBy(order.orderList);
      sqlNode = select;
    }

    SqlValidator validator = createSqlValidator();
    validator.validate(sqlNode);

    return plan(validator, sqlNode);
  }

  protected RelNode plan(SqlValidator validator, SqlNode sqlNode) {
    return planRoot(validator, sqlNode).rel;
  }

  protected RelRoot planRoot(SqlValidator validator, SqlNode sqlNode) {
    SqlToRelConverter sqlToRelConverter = createSqlToRelConverter(validator);

    RelRoot root;
    root = sqlToRelConverter.convertQuery(sqlNode, false, true);
    final RelBuilder relBuilder = createRelBuilder();
    root = root.withRel(
        RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    return root;
  }

  /* Optimize */

  public RelNode runStage(OptimizationStage stage, RelNode relNode) {
    return RelStageRunner.runStage(stage, relNode, this.planner);
  }

  /* Convert */

  public SqlNode convertToDialect(Dialect dialect, RelNode relNode) {
    switch (dialect) {
      case POSTGRES:
        return new PostgresSqlConverter()
            .convert(relNode);
      case CALCITE:
      case SQRL:
      case FLINK:
      default:
        throw new RuntimeException("Unknown dialect");
    }
  }

  /* Compile */

  public EnumerableRel convertToEnumerableRel(RelNode relNode) {
    return (EnumerableRel)runStage(OptimizationStage.CALCITE_ENGINE, relNode);
  }

  @SneakyThrows
  protected String generateSource(String className, EnumerableRel enumerableRel, HashMap carryover) {
    EnumerableRelImplementor implementor = new EnumerableRelImplementor(createRexBuilder(),
        carryover/*note: must be mutable*/);
    ClassDeclaration classDeclaration = implementor.implementRoot(enumerableRel, EnumerableRel.Prefer.ARRAY);

    String s = Expressions.toString(classDeclaration.memberDeclarations, "\n", false);

    String source = "public class " + className + "\n"
        + "    implements " + ArrayBindable.class.getName()
        + ", " + Serializable.class.getName()
        + " {\n"
        + s + "\n"
        + "}\n";
    return source;
  }

  public ClassLoader compile(String className, EnumerableRel enumerableRel, HashMap<String, Object> carryover) {
    return compile(className, generateSource(className, enumerableRel, carryover), defaultClassDir.toPath());
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
      @SuppressWarnings("unchecked")
      final Class<ArrayBindable> clazz =
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
  public Enumerator<Object[]> interpertable(EnumerableRel enumerableRel, DataContextImpl dataContext) {
    Map<String, Object> map = new HashMap<>();
    Bindable bindable = EnumerableInterpretable.toBindable(map,
        CalcitePrepare.Dummy.getSparkHandler(false), enumerableRel, EnumerableRel.Prefer.ARRAY);
    Enumerator<Object[]> enumerator = bindable.bind(dataContext)
        .enumerator();
    return enumerator;
  }

  /* Rel to Sql */
  public SqlNode toSql(Dialect dialect, RelNode relNode) {
    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    return converter.visitRoot(relNode).asStatement();
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
    return new SqlToRelConverter((a, b, c, d)-> null,
        validator,
        catalogReader, getCluster(),
        convertletTable,
        SqrlConfigurations.sqlToRelConverterConfig.withTrimUnusedFields(false));
  }

  public RelBuilder createRelBuilder() {
    return new RelBuilder(null, this.cluster, this.catalogReader){};
  }

  public RexBuilder createRexBuilder() {
    return new RexBuilder(typeFactory);
  }

}
