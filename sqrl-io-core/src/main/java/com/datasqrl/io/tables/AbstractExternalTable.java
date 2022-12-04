package com.datasqrl.io.tables;

import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import lombok.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
@Getter
public class AbstractExternalTable {

  @NonNull
  protected final DataSystemConnector connector;
  @NonNull
  protected final TableConfig configuration;
  @EqualsAndHashCode.Include
  @NonNull
  protected final NamePath path;
  @NonNull
  protected final Name name;

  public String qualifiedName() {
    return path.toString();
  }

  public Digest getDigest() {
    return new Digest(path, configuration.getNameCanonicalizer());
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
