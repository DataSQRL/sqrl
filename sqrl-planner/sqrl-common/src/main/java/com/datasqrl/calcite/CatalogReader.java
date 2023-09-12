package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.schema.SQRLTable;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;

import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class CatalogReader extends CalciteCatalogReader {

  @Getter
  private final SqrlSchema schema;

  public CatalogReader(SqrlSchema rootSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config,
      SqrlNameMatcher nameMatcher) {
    super(rootSchema, nameMatcher, ImmutableList.of(List.of(), ImmutableList.of()), typeFactory, config);
    this.schema = rootSchema;
  }

  /**
   * Returns a SQRL preparing table, with fields shadowed
   */
  public RelOptTable getSqrlTable(List<String> names) {
    List<String> absolutePath = getSqrlAbsolutePath(names);
    Map<List<String>, String> collect = schema.getSqrlTables().stream()
        .filter(f->f.getVt() != null) //todo: Need to register query access tables for planning
        .collect(Collectors.toMap(f -> f.getPath().toStringList(),
            f -> ((ModifiableSqrlTable)f.getVTable()).getName()));

    String sysTableName = nameMatcher().get(collect, List.of(), absolutePath);
    if (sysTableName == null) {
      return null;
    }

    RelOptTable internalTable = getTable(List.of(sysTableName));

    return internalTable;
  }

  public List<String> getSqrlAbsolutePath(List<String> path) {
    List<String> rel = nameMatcher().get(schema.getRelationships(), path, List.of());
    return (rel == null) ? path : rel;
  }
}
