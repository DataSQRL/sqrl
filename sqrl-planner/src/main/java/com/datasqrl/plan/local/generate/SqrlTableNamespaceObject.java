package com.datasqrl.plan.local.generate;

import com.datasqrl.name.Name;
import com.datasqrl.plan.local.ScriptTableDefinition;
import lombok.Value;
import org.apache.calcite.schema.Table;

@Value
public class SqrlTableNamespaceObject implements TableNamespaceObject<ScriptTableDefinition> {
  Name name;
  ScriptTableDefinition table;
}
