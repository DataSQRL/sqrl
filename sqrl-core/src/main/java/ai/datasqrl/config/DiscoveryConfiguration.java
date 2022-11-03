package ai.datasqrl.config;

import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DiscoveryConfiguration {

  @Builder.Default
  @NonNull
  @NotNull @Valid
  MetaData metastore = new MetaData();
  @Builder.Default
  boolean monitorSources = true;

  public MetaData getMetastore() {
    return metastore;
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class MetaData {

    public static final String DEFAULT_DATABASE = "datasqrl";

    @Builder.Default
    @NonNull
    @NotNull
    String databaseName = DEFAULT_DATABASE;


  }


}
