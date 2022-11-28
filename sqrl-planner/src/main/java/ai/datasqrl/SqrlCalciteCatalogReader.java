package ai.datasqrl;

import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;

public class SqrlCalciteCatalogReader extends CalciteCatalogReader {

  @Getter
  private final SqrlCalciteSchema sqrlRootSchema;

  public SqrlCalciteCatalogReader(SqrlCalciteSchema rootSchema,
      List<String> defaultSchema,
      RelDataTypeFactory typeFactory,
      CalciteConnectionConfig config) {
    super(rootSchema, defaultSchema, typeFactory, config);
    this.sqrlRootSchema = rootSchema;
  }
}
