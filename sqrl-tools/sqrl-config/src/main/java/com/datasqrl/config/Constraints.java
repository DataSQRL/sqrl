package com.datasqrl.config;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.datasqrl.config.SqrlConfig.Value;
import com.datasqrl.error.NotYetImplementedException;


public interface Constraints {

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface MinLength {
    int min() default 0;
  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Regex {
    String match() default "";
  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Default {

  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface NotNull {

  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface NotEmpty {

  }

  static Value addConstraints(Field field, SqrlConfig.Value value) {
    var annotations = field.getDeclaredAnnotations();

    for (Annotation annotation : annotations) {
      if (annotation instanceof MinLength lengthAnnotation) {
        final var minLength = lengthAnnotation.min();
        value = value.validate(x -> ((String)x).length()>= minLength,
            "String needs to be at least length " + minLength);
      } else if (annotation instanceof NotNull) {
        value = value.validate(x -> x != null,
            "Value cannot be null");
      } else if (annotation instanceof NotEmpty) {
        value = value.validate(x -> {
              if (x instanceof String) {
				return StringUtils.isNotBlank((String)x);
			}
              if (x instanceof Collection) {
				return ((Collection)x).size()>0;
			}
              if (x.getClass().isArray()) {
				return Array.getLength(x)>0;
			}
              return true;
            },
            "Value cannot be empty");
      } else if (annotation instanceof Regex regex) {
        var pattern = Pattern.compile(regex.match());
        value = value.validate(x -> pattern.matcher((String)x).matches(), "String does not match pattern " + regex.match());
      } else if (annotation instanceof Default) {
        //Ignore, handled in SqrlCommonsConfig
      } else if (annotation.getClass().getName().startsWith(Constraints.class.getName())) {
        throw new NotYetImplementedException("Annotation not yet supported: " + annotation.getClass().getName());
      }
    }
    return value;
  }


}
