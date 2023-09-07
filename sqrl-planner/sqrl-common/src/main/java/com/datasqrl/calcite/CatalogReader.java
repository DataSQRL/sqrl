package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.validate.SqlNameMatcher;

public class CatalogReader extends CalciteCatalogReader {

  private final SqrlSchema schema;

  public CatalogReader(SqrlSchema rootSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    super(rootSchema, List.of(), typeFactory, config);
    this.schema = rootSchema;
  }

  @Override
  public PreparingTable getTable(List<String> names) {
    return super.getTable(names);
  }

  @Override
  public SqlNameMatcher nameMatcher() {
    return super.nameMatcher();
  }

  /**
   * Returns a SQRL preparing table, with fields shadowed
   */
  public SqrlPreparingTable getSqrlTable(List<String> names) {
    List<String> absolutePath = getSqrlAbsolutePath(names);
    String sysTableName = nameMatcher().get(schema.getInternalTables(), names, List.of());
    if (sysTableName == null) {
      return null;
    }

    RelOptTable internalTable = getTable(List.of(sysTableName));
    RelDataType sqrlType = SqrlRelBuilder.shadow(internalTable.getRowType());

    return new SqrlPreparingTable(this,
        absolutePath, sqrlType, internalTable);
  }

  public List<String> getSqrlAbsolutePath(List<String> path) {
    List<String> rel = nameMatcher().get(schema.getRelationships(), path, List.of());
    return (rel == null) ? path : rel;
  }
}
