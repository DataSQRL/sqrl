package com.datasqrl.schema.constraint;

import com.datasqrl.config.error.ErrorCollector;
import com.datasqrl.parse.tree.name.Name;
import com.datasqrl.schema.type.Type;
import java.util.Map;
import java.util.Optional;

public class NotNull implements Constraint {

  public static final Name NAME = Name.system("not_null");

  public static final NotNull INSTANCE = new NotNull();

  private NotNull() {
  }

  @Override
  public boolean satisfies(Object value) {
    if (value == null) {
      return false;
    }
    if (value.getClass().isArray()) {
      for (Object v : (Object[]) value) {
        if (value == null) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean appliesTo(Type type) {
    return true;
  }

  @Override
  public String toString() {
    return NAME.getDisplay();
  }

  @Override
  public Name getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> export() {
    return null;
  }


  public static class Factory implements Constraint.Factory {

    @Override
    public Name getName() {
      return NAME;
    }

    @Override
    public Optional<Constraint> create(Map<String, Object> parameters, ErrorCollector errors) {
      return Optional.of(INSTANCE);
    }

  }

}
