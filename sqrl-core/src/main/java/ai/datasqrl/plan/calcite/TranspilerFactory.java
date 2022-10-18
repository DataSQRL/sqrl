package ai.datasqrl.plan.calcite;

import ai.datasqrl.SqrlCalciteCatalogReader;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;

public class TranspilerFactory {
  static final SqlValidator.Config config = SqlValidator.Config.DEFAULT
      .withSqlConformance(SqrlConformance.INSTANCE)
      .withCallRewrite(true)
      .withIdentifierExpansion(false)
      .withColumnReferenceExpansion(false)
      .withTypeCoercionEnabled(false)
      .withLenientOperatorLookup(false);

  public static SqrlValidatorImpl createSqrlValidator(SqrlCalciteSchema schema,
      List<String> assignmentPath, boolean forcePathIdentifiers) {
    Properties p = new Properties();
    p.put(CalciteConnectionProperty.CASE_SENSITIVE.name(), false);


    SqrlValidatorImpl validator = new SqrlValidatorImpl(
        PlannerFactory.getOperatorTable(),
        new SqrlCalciteCatalogReader(schema, List.of(), PlannerFactory.getTypeFactory(),
            new CalciteConnectionConfigImpl(p).set(CalciteConnectionProperty.CASE_SENSITIVE,
                "false")),
        PlannerFactory.getTypeFactory(),
        config
        );
    validator.assignmentPath = assignmentPath;
    validator.forcePathIdentifiers = forcePathIdentifiers;
    return validator;
  }

  public static SqlValidator createSqlValidator(CalciteSchema schema) {
    SqlValidator validator = SqlValidatorUtil.newValidator(
        SqrlOperatorTable.instance(),
        new CalciteCatalogReader(schema, List.of(), PlannerFactory.getTypeFactory(),
            new CalciteConnectionConfigImpl(new Properties()).set(CalciteConnectionProperty.CASE_SENSITIVE,
                "false")),
        PlannerFactory.getTypeFactory(),
        config);

    return validator;
  }
}
