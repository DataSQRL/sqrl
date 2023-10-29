package com.datasqrl.calcite.sqrl;

import com.datasqrl.canonicalizer.NamePath;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public interface CatalogResolver {
  NamePath getSqrlAbsolutePath(List<String> path);
  RelOptTable getTableFromPath(List<String> names);
//  Optional<SqlUserDefinedTableFunction> getTableFunction(List<String> path);
  RelDataTypeFactory getTypeFactory();
}
