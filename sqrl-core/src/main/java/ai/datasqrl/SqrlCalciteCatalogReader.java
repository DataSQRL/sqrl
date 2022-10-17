package ai.datasqrl;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

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

  @Override
  public PreparingTable getTableForMember(List<String> names) {
    return this.getTable(names);

//    SQRLTable table = super.getTableForMember(names).unwrap(SQRLTable.class);
//    VirtualRelationalTable vt = sqrlRootSchema.getMapping().get(table);
//    PreparingTable pt = this.getTable(List.of("", vt.getNameId()));
//    return pt;
  }

  public RelDataType getNamedType(SqlIdentifier typeName) {
    //We don't want to use the arbitrary nesting schema feature here
    if (typeName.names.size() > 1) return null;

    CalciteSchema.TypeEntry typeEntry = SqlValidatorUtil.getTypeEntry(this.getRootSchema(), typeName);
    return typeEntry != null ? (RelDataType)typeEntry.getType().apply(this.typeFactory) : null;
  }
}
