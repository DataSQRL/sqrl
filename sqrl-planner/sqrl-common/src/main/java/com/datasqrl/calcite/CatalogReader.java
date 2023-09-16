package com.datasqrl.calcite;

import com.datasqrl.schema.NamedTable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class CatalogReader extends CalciteCatalogReader {

  @Getter
  private final SqrlSchema schema;
  private final SqrlNameMatcher matcher;

  public CatalogReader(SqrlSchema rootSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config,
      SqlNameMatcher nameMatcher, SqrlNameMatcher matcher) {
    super(rootSchema, nameMatcher, ImmutableList.of(List.of(), ImmutableList.of()), typeFactory, config);
    this.schema = rootSchema;
    this.matcher = matcher;
  }

  public RelOptTable getSqrlTable(List<String> names) {
    List<String> absolutePath = getSqrlAbsolutePath(names);
    Map<List<String>, String> collect = schema.getSqrlTables().stream()
        .collect(Collectors.toMap(f -> f.getPath().toStringList(),
            f -> ((NamedTable)f.getRelOptTable()).getNameId()));

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
