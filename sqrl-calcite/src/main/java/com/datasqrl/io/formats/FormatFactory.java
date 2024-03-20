package com.datasqrl.io.formats;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.formats.Format.BaseFormat;
import java.util.Objects;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;

public interface FormatFactory {

  String getName();

  boolean isExtension(String formatExtension);

  Format fromConfig(SqrlConfig connectorConfig);

  Format createDefault();

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
      if (!(o instanceof FormatFactory)) {
        return false;
      }
      FormatFactory that = (FormatFactory) o;
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
