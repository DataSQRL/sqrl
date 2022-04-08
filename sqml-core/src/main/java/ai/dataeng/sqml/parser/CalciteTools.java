package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.planner.DagAssembler;
import java.util.Collections;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CachingSqrlSchema2;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader2;
import org.apache.calcite.rex.RexBuilder;

public class CalciteTools {

  public static DagAssembler getAssembler() {
    return new DagAssembler();
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
  public static CalciteCatalogReader getCalciteCatalogReader(CachingSqrlSchema2 schema) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    SqrlCalciteCatalogReader2 catalogReader = new SqrlCalciteCatalogReader2(schema,
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

}
