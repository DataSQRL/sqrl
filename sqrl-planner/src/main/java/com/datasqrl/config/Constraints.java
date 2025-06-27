/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.config;

import com.datasqrl.config.SqrlConfig.Value;
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

public interface Constraints {

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface MinLength {
    int min() default 0;
  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Regex {
    String match() default "";
  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Default {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface NotNull {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface NotEmpty {}

  static Value addConstraints(Field field, SqrlConfig.Value value) {
    var annotations = field.getDeclaredAnnotations();

    for (Annotation annotation : annotations) {
      if (annotation instanceof MinLength lengthAnnotation) {
        final var minLength = lengthAnnotation.min();
        value =
            value.validate(
                x -> ((String) x).length() >= minLength,
                "String needs to be at least length " + minLength);
      } else if (annotation instanceof NotNull) {
        value = value.validate(x -> x != null, "Value cannot be null");
      } else if (annotation instanceof NotEmpty) {
        value =
            value.validate(
                x -> {
                  if (x instanceof String string) {
                    return StringUtils.isNotBlank(string);
                  }
                  if (x instanceof Collection collection) {
                    return collection.size() > 0;
                  }
                  if (x.getClass().isArray()) {
                    return Array.getLength(x) > 0;
                  }
                  return true;
                },
                "Value cannot be empty");
      } else if (annotation instanceof Regex regex) {
        var pattern = Pattern.compile(regex.match());
        value =
            value.validate(
                x -> pattern.matcher((String) x).matches(),
                "String does not match pattern " + regex.match());
      } else if (annotation instanceof Default) {
        // Ignore, handled in SqrlCommonsConfig
      } else if (annotation.getClass().getName().startsWith(Constraints.class.getName())) {
        throw new UnsupportedOperationException(
            "Annotation not yet supported: " + annotation.getClass().getName());
      }
    }
    return value;
  }
}
