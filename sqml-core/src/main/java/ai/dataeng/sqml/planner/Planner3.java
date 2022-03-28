package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.parser.CalciteTableFactory;
import ai.dataeng.sqml.parser.QualifiedNode;
import ai.dataeng.sqml.parser.RelDataTypeFieldFactory;
import ai.dataeng.sqml.parser.RelToSql;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.VersionedName;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CachingSqrlSchema;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

public class Planner3 {

  private SqrlCalciteCatalogReader catalogReader;

  /**
   * Approach:
   * 1. Build one big plan dag w/ flinky nodes
   * 2. Assign 'LogicalSink' to all table labels
   * 3. 'Optimize', e.g. do nothing
   * 4. Cut graph at sources table labels -> pass to flink table api (replace w/ sink & scan?)
   * 5. Generate views at table labels on cut graph
   */
  public Result plan(List<QualifiedNode> nodes, Namespace namespace) {
    final Map<VersionedName, RelNode> plan = new HashMap<>();

    Pair<SqlValidator, SqlToRelConverter> cat = buildCalciteCatalog(namespace);
    SqlValidator validator = cat.getLeft();
    SqlToRelConverter converter = cat.getRight();

    RelShuttleImpl dagAssembler = new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        VersionedName name = VersionedName.parse(scan.getTable().getQualifiedName().get(0));
        if (plan.containsKey(name)) {
          RexBuilder rexBuilder = converter.getRexBuilder();
          RelNode relNode = plan.get(name);
          //Map over to join
          List<RexNode> inputs = new ArrayList<>();
          List<RelDataTypeField> fieldNames = scan.getRowType().getFieldList();
          List<String> relNodeNames = relNode.getRowType().getFieldNames().stream()
              .map(f->{
                if (!f.contains(VersionedName.ID_DELIMITER)) { //todo?
                  return f + VersionedName.ID_DELIMITER + "0";
                }
                return f;
              }).collect(Collectors.toList());
          for (int i = 0; i < fieldNames.size(); i++) {
            String fName = fieldNames.get(i).getName();
            int index =relNodeNames.indexOf(fName);
            if (index == -1) {
            } else {
              if (!fieldNames.get(i).getType().isNullable() &&
                  relNode.getRowType().getFieldList().get(i).getType().isNullable()) {
                inputs.add(rexBuilder.makeNotNull(RexInputRef.of(index, relNode.getRowType())));
              } else {
                inputs.add(RexInputRef.of(index, relNode.getRowType()));
              }
            }
          }


          RelBuilder relBuilder = converter.config.getRelBuilderFactory().create(converter.getCluster(), catalogReader);
          RelNode fin = relBuilder.push(relNode)
              .project(inputs)
              .build();
          return fin;
        }

        return super.visit(scan);
      }
    };

    for (QualifiedNode node : nodes) {
      System.out.println(node.getSqlNode());
      SqlNode scoped = validator.validate(node.getSqlNode());

      // Convert the valid AST into a logical plan
      RelNode logPlan = converter.convertQuery(scoped, false, true).rel;

      // Display the logical plan
      System.out.println(
          RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES));

//      RelOptPlanner planner = converter.getCluster().getPlanner();
//      planner.setRoot(logPlan);
//      planner.findBestExp()


      RelNode assembled = logPlan.accept(dagAssembler);

      VersionedName tableName = node.getTableName();
      plan.put(tableName, assembled);
    }

    List<RelNode> optimized = optimize(plan.values());

    System.out.println(plan);
    return new Result(plan, Map.of());
  }

  private List<RelNode> optimize(Collection<RelNode> values) {
    return null;
  }

  public Pair<SqlValidator, SqlToRelConverter> buildCalciteCatalog(Namespace namespace) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    RelDataTypeFieldFactory relDataTypeFactory = new RelDataTypeFieldFactory(typeFactory);
    SqrlCalciteCatalogReader catalogReader = new SqrlCalciteCatalogReader(
        new CachingSqrlSchema(new SqrlSchema(new CalciteTableFactory(Optional.empty(), namespace, relDataTypeFactory, false))),
        Collections.singletonList(""),
        typeFactory, config);
    this.catalogReader = catalogReader;

    RelOptPlanner planner = new HepPlanner(HepProgram.builder().build());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

    //Re-validate?
    SqlValidator validator = getValidator(catalogReader, typeFactory, SqrlOperatorTable.instance());

    SqlToRelConverter relConverter = new SqlToRelConverter(
        (rowType, queryString, schemaPath
            , viewPath) -> null,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());


    return Pair.of(validator, relConverter);
  }

  public SqlValidator getValidator(Optional<NamePath> context, Namespace namespace) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    SqrlCalciteCatalogReader catalogReader = getCalciteCatalogReader(context, namespace, typeFactory);
    SqlValidator validator = getValidator(catalogReader, typeFactory, SqrlOperatorTable.instance());
    return validator;
  }

  private SqlValidator getValidator(SqrlCalciteCatalogReader catalogReader,
      RelDataTypeFactory typeFactory, ReflectiveSqlOperatorTable operatorTable) {
    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
        .withCallRewrite(true)
        .withIdentifierExpansion(true)
        .withColumnReferenceExpansion(true)
        .withLenientOperatorLookup(true)
        .withSqlConformance(SqlConformanceEnum.LENIENT)
        ;

    SqlValidator validator = new SqrlValidator(
        operatorTable,
        catalogReader,
        typeFactory,
        validatorConfig);
    return validator;
  }

  private SqrlCalciteCatalogReader getCalciteCatalogReader(Optional<NamePath> context,
      Namespace namespace, SqrlTypeFactory typeFactory) {
    CalciteTableFactory calciteTableFactory = new CalciteTableFactory(context, namespace, new RelDataTypeFieldFactory(typeFactory), false);
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

  /**
   *
   */
  @Value
  public class Result {
    Map<VersionedName, RelNode> planRoots;
    Map<VersionedName, RelNode> tableLabels;
  }
}
