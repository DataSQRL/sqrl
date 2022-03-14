package org.apache.calcite.prepare;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CachingSqrlSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/**
 * Delegates a PreparingTable to SqrlRelOptTable.
 *
 * This is only valid per-query
 */
@Slf4j
public class SqrlCalciteCatalogReader extends CalciteCatalogReader {

  private final CachingSqrlSchema rootSqrlSchema;

  public SqrlCalciteCatalogReader(CachingSqrlSchema rootSchema,
      List<String> defaultSchema,
      RelDataTypeFactory typeFactory,
      CalciteConnectionConfig config) {
    super(rootSchema, defaultSchema, typeFactory, config);
    this.rootSqrlSchema = rootSchema;
  }

  @Override
  public PreparingTable getTable(List<String> names) {
    CalciteSchema.TableEntry entry = SqlValidatorUtil.getTableEntry(this, names);
    if (entry != null) {
      final Table table = entry.getTable();
      return new SqrlRelOptTable(RelOptTableImpl.create(this,
          table.getRowType(typeFactory), entry, null));
    }
    return null;
  }

  /**
   * A workaround for calcite to expand JOIN paths. Provides the scope
   * of the current query to lookup aliases.
   */
  public void setScope(ListScope scope) {
    rootSqrlSchema.setScope(scope);
  }
}
