package com.datasqrl.config;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;


public interface Constraints {

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface MinLength {
    int min() default 0;
  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Default {

  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface NotNull {

  }

  static SqrlConfig.Value addConstraints(Field field, SqrlConfig.Value value) {
    Annotation[] annotations = field.getDeclaredAnnotations();

    for (Annotation annotation : annotations) {
      if (annotation instanceof MinLength) {
        MinLength lengthAnnotation = (MinLength) annotation;
        value = value.validate(x -> ((String)x).length()>= lengthAnnotation.min(),
            "String needs to be at least length " + lengthAnnotation.min());
      } else if (annotation instanceof NotNull) {
        value = value.validate(x -> x != null,
            "Value cannot be null");
      }
    }
    return value;
  }


}
