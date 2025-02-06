package com.datasqrl.config;

import java.util.Objects;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Getter;

public interface FormatFactory {

  String getName();

  boolean isExtension(String formatExtension);

  TableConfig.Format fromConfig(PackageJson.EngineConfig connectorConfig);

  TableConfig.Format createDefault();

  @AllArgsConstructor
  @Getter
  public static abstract class BaseFormatFactory implements FormatFactory {

    private final String name;
    private final Set<String> extensions;

    @Override
    public boolean isExtension(String formatExtension) {
      return extensions.contains(formatExtension.toLowerCase());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FormatFactory that)) {
        return false;
      }
      return name.equalsIgnoreCase(that.getName());
    }

    @Override
    public int hashCode() {
      return Objects.hash(name.toLowerCase());
    }

    @Override
    public String toString() {
      return name;
    }

  }

}
