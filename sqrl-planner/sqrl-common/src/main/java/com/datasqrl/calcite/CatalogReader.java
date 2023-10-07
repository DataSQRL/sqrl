package com.datasqrl.calcite;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class CatalogReader extends CalciteCatalogReader {

  @Getter
  private final SqrlSchema schema;

  public CatalogReader(SqrlSchema rootSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    super(rootSchema, SqlNameMatchers.withCaseSensitive(false), ImmutableList.of(List.of(), ImmutableList.of()), typeFactory, config);
    this.schema = rootSchema;
  }

  public RelOptTable getTableFromPath(List<String> names) {
    List<String> absolutePath = getSqrlAbsolutePath(names);
    String sysTableName = nameMatcher().get(schema.getPathToTableMap(), List.of(), absolutePath);
    if (sysTableName == null) {
      return null;
    }

    return getTable(List.of(sysTableName));
  }

  public List<String> getSqrlAbsolutePath(List<String> path) {
    List<String> rel = nameMatcher().get(schema.getAbsolutePathMap(), path, List.of());
    return (rel == null) ? path : rel;
  }
}
