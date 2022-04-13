package ai.dataeng.sqml.parser;

import java.util.Collections;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader2;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

public class CalciteTools {

  public static CalciteCatalogReader getCalciteCatalogReader(SqrlCalciteSchema schema) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    SqrlCalciteCatalogReader2 catalogReader = new SqrlCalciteCatalogReader2(schema,
        Collections.singletonList(""),
        typeFactory, config);
    return catalogReader;
  }

  public static RelOptCluster createHepCluster(RelDataTypeFactory typeFactory) {
    RelOptPlanner planner = new HepPlanner(HepProgram.builder().build());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

    return cluster;
  }

}
