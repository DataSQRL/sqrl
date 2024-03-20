package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NamePath;
import java.util.List;
import java.util.Optional;
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

  public Optional<RelOptTable> getTableFromPath(NamePath names) {
    NamePath absolutePath = getSqrlAbsolutePath(names);
    String sysTableName = schema.getPathToSysTableMap().get(absolutePath);
    if (sysTableName == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(getTable(List.of(sysTableName)));
  }

  public NamePath getSqrlAbsolutePath(NamePath path) {
    NamePath rel = schema.getPathToAbsolutePathMap().get(path);
    return (rel == null) ? path : rel;
  }
}
