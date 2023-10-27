package com.datasqrl.calcite;

import com.datasqrl.calcite.sqrl.CatalogResolver;
import com.datasqrl.canonicalizer.NameCanonicalizer;
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
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class CatalogReader extends CalciteCatalogReader implements CatalogResolver {

  @Getter
  private final SqrlSchema schema;

  public CatalogReader(SqrlSchema rootSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    super(rootSchema, SqlNameMatchers.withCaseSensitive(false), ImmutableList.of(List.of(), ImmutableList.of()), typeFactory, config);
    this.schema = rootSchema;
  }

  public RelOptTable getTableFromPath(List<String> names) {
    NamePath absolutePath = getSqrlAbsolutePath(NamePath.system(names));
    String sysTableName = schema.getPathToSysTableMap().get(absolutePath);
    if (sysTableName == null) {
      return null;
    }

    return getTable(List.of(sysTableName));
  }

  public NamePath getSqrlAbsolutePath(List<String> path) {
    return getSqrlAbsolutePath(NamePath.system(path));
  }

  public NamePath getSqrlAbsolutePath(NamePath path) {
    NamePath rel = schema.getPathToAbsolutePathMap().get(path);
    return (rel == null) ? path : rel;
  }

  public Optional<SqlUserDefinedTableFunction> getTableFunction(List<String> path) {
    if (path.isEmpty()) {
      return Optional.empty();
    }

    return SqrlNameMatcher.getLatestVersion(NameCanonicalizer.SYSTEM, schema.plus().getFunctionNames(),
            String.join(".", path))
        .flatMap(s -> this.getOperatorList().stream()
        .filter(f -> f.getName().equalsIgnoreCase(s))
        .filter(f -> f instanceof SqlUserDefinedTableFunction)
        .map(f -> (SqlUserDefinedTableFunction) f)
        .findFirst());
  }

}
