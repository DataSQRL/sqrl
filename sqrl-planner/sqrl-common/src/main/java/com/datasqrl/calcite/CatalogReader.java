package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.SQRLTable;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
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
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class CatalogReader extends CalciteCatalogReader {

  @Getter
  private final SqrlSchema schema;

  public CatalogReader(SqrlSchema rootSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    super(rootSchema, createNameMatcher(), ImmutableList.of(List.of(), ImmutableList.of()), typeFactory, config);
    this.schema = rootSchema;
  }

  private static SqlNameMatcher createNameMatcher() {
    return new SqrlNameMatcher(NameCanonicalizer.SYSTEM);
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
    for (SQRLTable table : schema.getSqrlTables()) {
      System.out.println(table);
      System.out.println(table.getVt());
    }
    Map<List<String>, String> collect = schema.getSqrlTables().stream()
        .filter(f->f.getVt() != null) //todo: Need to register query access tables for planning
        .collect(Collectors.toMap(f -> f.getPath().toStringList(),
            f -> ((ModifiableSqrlTable)f.getVTable()).getName()));

    String sysTableName = nameMatcher().get(collect, List.of(), absolutePath);
    if (sysTableName == null) {
      return null;
    }

    RelOptTable internalTable = getTable(List.of(sysTableName));
    RelDataType sqrlType = internalTable.getRowType();

    return new SqrlPreparingTable(this,
        absolutePath, sqrlType, internalTable);
  }

  public List<String> getSqrlAbsolutePath(List<String> path) {
    List<String> rel = nameMatcher().get(schema.getRelationships(), path, List.of());
    return (rel == null) ? path : rel;
  }
}
