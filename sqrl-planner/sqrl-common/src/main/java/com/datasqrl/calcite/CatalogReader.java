package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NamePath;
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

    NamePath absolutePath = getSqrlAbsolutePath(NamePath.system(names));
    String sysTableName = schema.getPathToTableMap().get(absolutePath);
    if (sysTableName == null) {
      return null;
    }

    return getTable(List.of(sysTableName));
  }

  public NamePath getSqrlAbsolutePath(List<String> path) {
    return getSqrlAbsolutePath(NamePath.system(path));
  }

  public NamePath getSqrlAbsolutePath(NamePath path) {
    NamePath rel = schema.getAbsolutePathMap().get(path);
    return (rel == null) ? path : rel;
  }
}
