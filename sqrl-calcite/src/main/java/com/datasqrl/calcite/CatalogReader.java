package com.datasqrl.calcite;

import java.util.List;
import java.util.Optional;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NamePath;

import lombok.Getter;

public class CatalogReader extends CalciteCatalogReader {

  @Getter
  private final SqrlSchema schema;

  public CatalogReader(SqrlSchema rootSchema, TypeFactory typeFactory, CalciteConnectionConfig config) {
    super(rootSchema, SqlNameMatchers.withCaseSensitive(false), ImmutableList.of(List.of(), ImmutableList.of()), typeFactory, config);
    this.schema = rootSchema;
  }

  public Optional<RelOptTable> getTableFromPath(NamePath names) {
    var absolutePath = getSqrlAbsolutePath(names);
    var sysTableName = schema.getPathToSysTableMap().get(absolutePath);
    if (sysTableName == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(getTable(List.of(sysTableName)));
  }

  public NamePath getSqrlAbsolutePath(NamePath path) {
    var rel = schema.getPathToAbsolutePathMap().get(path);
    return (rel == null) ? path : rel;
  }
}
