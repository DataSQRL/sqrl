package com.datasqrl.plan.local.generate;

import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public class Debugger {

  public static final Debugger NONE = new Debugger(DebuggerConfig.NONE, null);

  @NonNull private final DebuggerConfig config;
  private final ModuleLoader moduleLoader;

  public boolean isDebugTable(Name tableName) {
    return config.debugTable(tableName);
  }

  public TableSink getDebugSink(Name sinkName, ErrorCollector errors) {
    NamePath sinkPath = config.getSinkBasePath().concat(sinkName);
    return LoaderUtil.loadSink(sinkPath, errors, moduleLoader);
  }

}
