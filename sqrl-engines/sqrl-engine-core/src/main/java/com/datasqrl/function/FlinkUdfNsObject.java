package com.datasqrl.function;

import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.canonicalizer.Name;
import java.net.URL;
import java.util.Optional;
import lombok.Value;
import org.apache.flink.table.functions.UserDefinedFunction;

@Value
public class FlinkUdfNsObject implements FunctionNamespaceObject<UserDefinedFunction> {
  Name name;
  UserDefinedFunction function;
  Optional<URL> jarUrl;
}
