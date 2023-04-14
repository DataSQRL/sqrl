package com.datasqrl.plan.local.generate;

import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.local.ScriptTableDefinition;
import lombok.NonNull;
import lombok.Value;

@Value
public class SqrlTableNamespaceObject implements TableNamespaceObject<ScriptTableDefinition> {
  @NonNull
  Name name;
  @NonNull
  ScriptTableDefinition table;
}
