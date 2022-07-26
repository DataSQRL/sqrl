package ai.datasqrl.plan.calcite;

import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;

public class TranspilerFactory {

  public static SqrlValidatorImpl createSqrlValidator(CalciteSchema schema) {
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
    return validator;
  }

  public static SqlValidator createSqlValidator(CalciteSchema schema) {
    SqlValidator validator = SqlValidatorUtil.newValidator(
        SqlStdOperatorTable.instance(),
        new CalciteCatalogReader(schema, List.of(), new SqrlTypeFactory(new SqrlTypeSystem()),
            new CalciteConnectionConfigImpl(new Properties()).set(CalciteConnectionProperty.CASE_SENSITIVE,
                "false")),
        new SqrlTypeFactory(new SqrlTypeSystem()),
        SqlValidator.Config.DEFAULT.withSqlConformance(SqrlConformance.INSTANCE)
            .withCallRewrite(true)
            .withIdentifierExpansion(false)
            .withColumnReferenceExpansion(false)
            .withTypeCoercionEnabled(false)
            .withLenientOperatorLookup(true));

    return validator;
  }
}
