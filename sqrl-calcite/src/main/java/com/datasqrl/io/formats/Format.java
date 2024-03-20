package com.datasqrl.io.formats;

import java.util.Objects;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

public interface Format {

  String getName();

  Optional<String> getSchemaType();

  @AllArgsConstructor
  @Getter
  abstract class BaseFormat implements Format {

    @NonNull String name;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Format)) {
        return false;
      }
      Format that = (Format) o;
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

    @Override
    public Optional<String> getSchemaType() {
      return Optional.empty();
    }
  }

  class DefaultFormat extends BaseFormat {

    public DefaultFormat(@NonNull String name) {
      super(name);
    }

  }
}
