package ai.dataeng.sqml.type;

import java.util.Locale;

public abstract class ScalarType extends Type {

  protected ScalarType(String name) {
    super(name);
  }

  public static ScalarType fromName(String type) {
    switch (type.trim().toLowerCase(Locale.ENGLISH)) {
      case "integer": return IntegerType.INSTANCE;
      case "boolean": return BooleanType.INSTANCE;
      case "string": return StringType.INSTANCE;
      case "float": return FloatType.INSTANCE;
      case "datetime": return DateTimeType.INSTANCE;
      case "uuid": return UuidType.INSTANCE;
    }
    return null;
  }
}
