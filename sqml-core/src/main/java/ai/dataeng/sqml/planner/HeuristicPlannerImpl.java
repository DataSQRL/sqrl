package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.planner.operator2.SqrlAggregate;
import ai.dataeng.sqml.planner.operator2.SqrlFilter;
import ai.dataeng.sqml.planner.operator2.SqrlJoin;
import ai.dataeng.sqml.planner.operator2.SqrlProject;
import ai.dataeng.sqml.planner.operator2.SqrlSort;
import ai.dataeng.sqml.planner.operator2.SqrlTableScan;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CachingSqrlSchema;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.AggregateFactory;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.core.RelFactories.JoinFactory;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.core.RelFactories.SortFactory;
import org.apache.calcite.rel.core.RelFactories.TableScanFactory;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.sql.FlattenTableNames;
import org.apache.calcite.sql.OperatorTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.tools.SqrlRelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

public class HeuristicPlannerImpl implements Planner {

  /**
   * Calcite has some hard coded logic w/ Logical plan tables therefore we convert it manually
   * after processing rather than passed in the relation builder.
   */
  private RelNode convertToSqrl(RelNode node) {
    if (node instanceof Project) {
      Project project = (Project) node;
      RelNode input = convertToSqrl(project.getInput());
      return SqrlProject.create(input, project.getHints(), project.getProjects(), project.getRowType());
    } else if (node instanceof TableScan) {
      TableScan scan = (TableScan) node;
      return SqrlTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints());
    } else if (node instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) node;
      RelNode input = convertToSqrl(aggregate.getInput());
      return SqrlAggregate.create(input, aggregate.getHints(), aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
    } else if (node instanceof Sort) {
      Sort sort = (Sort) node;
      RelNode input = convertToSqrl(sort.getInput());
      return SqrlSort.create(input, sort.getCollation(), sort.offset, sort.fetch);
    } else if (node instanceof Join) {
      Join join = (Join) node;
      RelNode left = convertToSqrl(join.getLeft());
      RelNode right = convertToSqrl(join.getRight());
      return SqrlJoin.create(left, right, join.getHints(), join.getCondition(), join.getVariablesSet(), join.getJoinType());
    } else if (node instanceof Filter) {
      Filter filter = (Filter) node;
      RelNode input = convertToSqrl(filter.getInput());
      return SqrlFilter.create(input, filter.getCondition(), filter.getVariablesSet());
    } else if (node instanceof Union) {
//      Union filter = (Union) node;
//      return SqrlU.create();
    }
    throw new RuntimeException("?" + node.getClass());
  }

  @Override
  @SneakyThrows
  public PlannerResult plan(Optional<NamePath> context, Namespace namespace, String sql) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    SqrlCalciteCatalogReader catalogReader = getCalciteCatalogReader(context, namespace, typeFactory);
    SqlValidator validator = getValidator(catalogReader, typeFactory);

    SqlNode sqlNode = parse(sql);
    SqlNode validNode = validator.validate(sqlNode);
    //Expand table names to fully expanded sql nodes
    SqlNode expanded = macroExpand(validNode, validator);
    SqlValidator newValidator = getValidator(catalogReader, typeFactory);
    newValidator.validate(expanded);

    RelOptPlanner planner = getPlanner(namespace, catalogReader);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

    //TODO: We could use a different catalog reader now that we don't have to expand table names
    SqlToRelConverter relConverter = getSqlToRelConverter(newValidator, cluster, catalogReader);
    RelRoot root = relConverter.convertQuery(validNode, false, true);

    cluster.getPlanner().setRoot(root.rel);
    RelNode optimizedNode = cluster.getPlanner().findBestExp();
    RelNode sqrl = convertToSqrl(optimizedNode);

    return new PlannerResult(sqrl, sqlNode, newValidator);
  }

  /**
   * Expands a validated SqlNode
   */
  private SqlNode macroExpand(SqlNode validNode,
      final SqlValidator validator) {
    SqrlMacroExpand macroExpand = new SqrlMacroExpand(validator);
    SqlSelect select = (SqlSelect) validNode;
    macroExpand.expand(select);
    return select;
  }

  private SqlToRelConverter getSqlToRelConverter(SqlValidator validator,
      RelOptCluster cluster, CalciteCatalogReader catalogReader) {
    Config converterConfig = SqlToRelConverter.config()
        .withExpand(false)
        .withCreateValuesRel(false)
        .withTrimUnusedFields(false);

    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        converterConfig);
    return relConverter;
  }

  @SneakyThrows
  private SqlNode parse(String sql) {
    SqlParser.Config parserConfig = SqlParser.config()
        .withParserFactory(SqlParser.config().parserFactory())
        .withLex(Lex.JAVA)
        .withConformance(SqlConformanceEnum.LENIENT)
        .withIdentifierMaxLength(256);
    SqlParser parser = SqlParser.create(sql, parserConfig);
    SqlNode sqlNode = parser.parseQuery();

    // Validate the initial AST
    FlattenTableNames.rewrite(sqlNode);

    return sqlNode;
  }

  private SqlValidator getValidator(SqrlCalciteCatalogReader catalogReader,
      RelDataTypeFactory typeFactory) {
    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
        .withCallRewrite(true)
        .withIdentifierExpansion(true)
        .withColumnReferenceExpansion(true)
        .withLenientOperatorLookup(true)
        .withSqlConformance(SqlConformanceEnum.LENIENT)
        ;

    SqlValidator validator = new SqrlValidator(
        OperatorTable.instance(),
        catalogReader,
        typeFactory,
        validatorConfig);
    return validator;
  }

  private SqrlCalciteCatalogReader getCalciteCatalogReader(Optional<NamePath> context,
      Namespace namespace, SqrlTypeFactory typeFactory) {
    CalciteTableFactory calciteTableFactory = new CalciteTableFactory(context, namespace, new RelDataTypeFieldFactory(typeFactory));
    CachingSqrlSchema schema = new CachingSqrlSchema(new SqrlSchema(calciteTableFactory));

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    SqrlCalciteCatalogReader catalogReader = new SqrlCalciteCatalogReader(schema,
        Collections.singletonList(""),
        typeFactory, config);
    return catalogReader;
  }

  @Override
  public SqrlRelBuilder getRelBuilder(Optional<NamePath> context, Namespace namespace) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    SqrlCalciteCatalogReader catalogReader = getCalciteCatalogReader(context, namespace, typeFactory);

    RelOptPlanner planner = getPlanner(namespace, catalogReader);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

    RelBuilderFactory relBuilder = getRelBuilderFactory();

    return (SqrlRelBuilder)relBuilder.create(cluster, catalogReader);
  }

  @Override
  public SqlValidator getValidator(Namespace namespace) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    SqrlCalciteCatalogReader catalogReader = getCalciteCatalogReader(Optional.empty(), namespace, typeFactory);
    SqlValidator validator = getValidator(catalogReader, typeFactory);
    return validator;
  }

  public RelBuilderFactory getRelBuilderFactory() {
    RelBuilder.Config cfg = RelBuilder.Config.DEFAULT.withBloat(100);
    return (cluster, catalogReader) -> new SqrlRelBuilder(Contexts.of(
        new ProjectFactoryImpl(),
        new AggregateFactoryImpl(),
        new FilterFactoryImpl(),
        new JoinFactoryImpl(),
        new SortFactoryImpl(),
        new TableScanFactoryImpl(),
        NOOP_EXPANDER,
        cfg), cluster, catalogReader);
  }

  public class ProjectFactoryImpl implements ProjectFactory {
    @Override
    public RelNode createProject(RelNode input, List<RelHint> hints,
        List<? extends RexNode> childExprs, @Nullable List<? extends @Nullable String> fieldNames) {
      return SqrlProject.create(input, hints, childExprs, fieldNames);
    }
  }
  public class AggregateFactoryImpl implements AggregateFactory {

    @Override
    public RelNode createAggregate(RelNode input, List<RelHint> hints, ImmutableBitSet groupSet,
        ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      return SqrlAggregate.create(input, hints, groupSet, groupSets, aggCalls);
    }
  }
  public class FilterFactoryImpl implements FilterFactory {

    @Override
    public RelNode createFilter(RelNode input, RexNode condition, Set<CorrelationId> variablesSet) {
      return SqrlFilter.create(input, condition, variablesSet);
    }
  }
  public class JoinFactoryImpl implements JoinFactory {

    @Override
    public RelNode createJoin(RelNode left, RelNode right, List<RelHint> hints, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone) {
      return SqrlJoin.create(left, right, hints, condition, variablesSet, joinType);
    }
  }
  public class SortFactoryImpl implements SortFactory {

    @Override
    public RelNode createSort(RelNode input, RelCollation collation, @Nullable RexNode offset,
        @Nullable RexNode fetch) {
      return SqrlSort.create(input, collation, offset, fetch);
    }
  }
  public class TableScanFactoryImpl implements TableScanFactory {
    @Override
    public RelNode createScan(ToRelContext toRelContext, RelOptTable table) {
      return table.toRel(toRelContext);
    }
  }

  private static RelOptPlanner getPlanner(Namespace namespace,
      SqrlCalciteCatalogReader catalogReader) {
    HepProgram hep = HepProgram.builder()
//        .addRuleClass(TableNameRewriterRule.class)
        .build();
    HepPlanner hepPlanner = new HepPlanner(hep);
//    hepPlanner.addRule(new TableNameRewriterRule(TableNameRewriterRule.Config.DEFAULT, namespace, catalogReader));
    hepPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return hepPlanner;
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
      , viewPath) -> null;
}
