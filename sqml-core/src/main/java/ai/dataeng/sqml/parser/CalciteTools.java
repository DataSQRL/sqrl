package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import ai.dataeng.sqml.planner.DagAssembler;
import ai.dataeng.sqml.planner.LogicalPlanDag;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.VersionedName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CachingSqrlSchema;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlValidator;
import org.apache.calcite.tools.RelBuilder;

public class CalciteTools {

  public static DagAssembler getAssembler(Namespace namespace) {
    return new DagAssembler(namespace);
  }
//
//  RelShuttleImpl dagAssembler = new RelShuttleImpl() {
//    @Override
//    public RelNode visit(TableScan scan) {
//
//
////
////      VersionedName name = VersionedName.parse(scan.getTable().getQualifiedName().get(0));
////      if (plan.containsKey(name)) {
////        RexBuilder rexBuilder = converter.getRexBuilder();
////        RelNode relNode = plan.get(name);
////        //Map over to join
////        List<RexNode> inputs = new ArrayList<>();
////        List<RelDataTypeField> fieldNames = scan.getRowType().getFieldList();
////        List<String> relNodeNames = relNode.getRowType().getFieldNames().stream()
////            .map(f->{
////              if (!f.contains(VersionedName.ID_DELIMITER)) { //todo?
////                return f + VersionedName.ID_DELIMITER + "0";
////              }
////              return f;
////            }).collect(Collectors.toList());
////        for (int i = 0; i < fieldNames.size(); i++) {
////          String fName = fieldNames.get(i).getName();
////          int index =relNodeNames.indexOf(fName);
////          if (index == -1) {
////          } else {
////            if (!fieldNames.get(i).getType().isNullable() &&
////                relNode.getRowType().getFieldList().get(i).getType().isNullable()) {
////              inputs.add(rexBuilder.makeNotNull(RexInputRef.of(index, relNode.getRowType())));
////            } else {
////              inputs.add(RexInputRef.of(index, relNode.getRowType()));
////            }
////          }
////        }
////
////
////        RelBuilder relBuilder = converter.config.getRelBuilderFactory().create(converter.getCluster(), catalogReader);
////        RelNode fin = relBuilder.push(relNode)
////            .project(inputs)
////            .build();
////        return fin;
////      }
//
//      return super.visit(scan);
//    }
//  };
public static SqrlCalciteCatalogReader getCalciteCatalogReader(LogicalDag dag) {
//  CalciteTableFactory calciteTableFactory = new CalciteTableFactory(context, namespace, new RelDataTypeFieldFactory(typeFactory), false);
//  CachingSqrlSchema schema = new CachingSqrlSchema(new SqrlSchema(calciteTableFactory));
//
//  // Configure and instantiate validator
//  Properties props = new Properties();
//  props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
//  CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
//  SqrlCalciteCatalogReader catalogReader = new SqrlCalciteCatalogReader(schema,
//      Collections.singletonList(""),
//      typeFactory, config);
  return null;
}
  public static SqrlCalciteCatalogReader getCalciteCatalogReader(Optional<NamePath> context,
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

  public static RelOptCluster createHepCluster(SqrlTypeFactory typeFactory) {
    RelOptPlanner planner = new HepPlanner(HepProgram.builder().build());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

    return cluster;
  }


  public static SqlValidator getValidator(SqrlCalciteCatalogReader catalogReader,
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

}
