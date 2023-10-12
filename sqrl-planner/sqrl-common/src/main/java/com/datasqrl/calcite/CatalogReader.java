package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
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

  public SqlUserDefinedTableFunction getTableFunction(List<String> path) {
    List<SqlOperator> result = new ArrayList<>();
    String tableFunctionName = String.join(".", path);
    //get latest function
    if (tableFunctionName.isEmpty()) {
      return null;
    }
    String latestVersionName = SqrlNameMatcher.getLatestVersion(NameCanonicalizer.SYSTEM,
        schema.plus().getFunctionNames(), tableFunctionName);
    if (latestVersionName == null) {
      //todo return optional
      return null;
    }

    Optional<SqlOperator> first = this.getOperatorList()
        .stream()
        .filter(f -> f.getName().equalsIgnoreCase(latestVersionName))
        .findFirst();
//    operatorTable.lookupOperatorOverloads(new SqlIdentifier(List.of(latestVersionName), SqlParserPos.ZERO),
//        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION, result, nameMatcher());

    if (first.isEmpty()) {
      return null;
    }

    return (SqlUserDefinedTableFunction)first.get();
  }

}
