/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class CompilerConfiguration {

  @NonNull @Valid
  @Builder.Default
  APIConfiguration api = new APIConfiguration();

  @NonNull @Valid
  @Builder.Default
  DebugConfiguration debug = new DebugConfiguration();

  @NonNull @NotEmpty
  @Builder.Default
  String errorSink = "print.errors";

  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Getter
  public static class APIConfiguration {

    @Builder.Default
    @Min(0) @Max(128)
    int maxArguments = 5;

  }

  public static class DebugConfiguration {

    @Builder.Default
    @NonNull @NotEmpty
    String debugSink = "print";

    @Builder.Default
    List<String> tables = null;


    public DebuggerConfig getDebugger() {
      NamePath sinkBasePath = NamePath.parse(debugSink);
      Set<Name> debugTables = null;
      if (tables!=null && !tables.isEmpty()) {
        debugTables = tables.stream().map(Name::system).collect(Collectors.toSet());
      }
      return DebuggerConfig.builder().enabled(true)
          .sinkBasePath(sinkBasePath)
          .debugTables(debugTables)
          .build();
    }

    public static ErrorLocation getLocation() {
      return ErrorPrefix.CONFIG.resolve("compiler").resolve("debug");
    }

  }

}
