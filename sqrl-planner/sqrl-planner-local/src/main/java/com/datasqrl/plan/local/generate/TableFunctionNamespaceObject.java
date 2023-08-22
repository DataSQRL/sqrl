package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.plan.local.ScriptTableDefinition;
import lombok.NonNull;
import lombok.Value;

@Value
public class TableFunctionNamespaceObject implements TableNamespaceObject<TableFunctionBase> {
  @NonNull
  Name name;
  @NonNull
  TableFunctionBase table;
}
