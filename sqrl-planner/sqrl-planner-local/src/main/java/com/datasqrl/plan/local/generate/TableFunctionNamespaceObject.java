package com.datasqrl.plan.local.generate;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.TableNamespaceObject;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.util.Optional;

@AllArgsConstructor
@Getter
public class TableFunctionNamespaceObject implements TableNamespaceObject<TableFunctionBase> {
  @NonNull
  Name name;
  @NonNull
  TableFunctionBase table;

  @Override
  public boolean apply(Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    throw new RuntimeException("Cannot import a table function");
  }
}
