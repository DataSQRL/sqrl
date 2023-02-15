package com.datasqrl.plan.local.generate;

import com.datasqrl.name.Name;
import java.net.URL;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

@Value
public class CalciteFunctionNsObject implements NamespaceObject {
  Name name;
  SqlFunction function;
}
