/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.spi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import lombok.*;

import javax.validation.constraints.NotEmpty;
import java.util.Optional;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class ManifestConfiguration {

  public static final String MANIFEST = "manifest";

  @NonNull @NotEmpty
  String main;
  String graphql;

  @JsonIgnore
  public Optional<String> getOptGraphQL() {
    return Optional.ofNullable(graphql)
        .filter(gql -> !gql.isEmpty());
  }

}
