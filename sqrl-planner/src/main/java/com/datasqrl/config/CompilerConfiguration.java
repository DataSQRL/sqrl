package com.datasqrl.config;

import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class CompilerConfiguration {

  @NonNull @Valid
  @Builder.Default
  APIConfiguration api = new APIConfiguration();


  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Getter
  public static class APIConfiguration {

    @Builder.Default
    @Min(0) @Max(128)
    int maxArguments = 5;

  }

}
