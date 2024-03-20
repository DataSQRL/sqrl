package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import java.util.Collections;
import java.util.Set;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class DebuggerConfig {

  public static final DebuggerConfig NONE = new DebuggerConfig(false, NamePath.ROOT, Collections.EMPTY_SET);

  boolean enabled;
  @NonNull NamePath sinkBasePath;
  @Builder.Default
  Set<Name> debugTables = null;

  public boolean debugTable(Name tableName) {
    return enabled && (debugTables==null || debugTables.contains(tableName));
  }

  public static DebuggerConfig of(@NonNull NamePath sinkBasePath, Set<Name> debugTables) {
    return DebuggerConfig.builder().enabled(true).sinkBasePath(sinkBasePath).debugTables(debugTables)
        .build();
  }

}
