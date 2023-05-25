/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import java.util.Optional;
import lombok.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
@Getter
public abstract class AbstractExternalTable {

  @NonNull
  protected final DataSystemConnectorSettings connector;
  @NonNull
  protected final TableConfig configuration;
  @EqualsAndHashCode.Include
  @NonNull
  protected final NamePath path;
  @NonNull
  protected final Name name;
  @NonNull
  protected final Optional<TableSchema> tableSchema;

  public String qualifiedName() {
    return path.toString();
  }

  public Digest getDigest() {
    return new Digest(path, configuration.getBase().getCanonicalizer());
  }

  @Value
  public static class Digest implements Serializable {

    private final NamePath path;
    private final NameCanonicalizer canonicalizer;

    public String toString(char delimiter, String... suffixes) {
      List<String> components = new ArrayList<>();
      path.stream().map(Name::getCanonical).forEach(components::add);
      for (String suffix : suffixes) {
        components.add(suffix);
      }
      return String.join(String.valueOf(delimiter), components);
    }

  }

}
