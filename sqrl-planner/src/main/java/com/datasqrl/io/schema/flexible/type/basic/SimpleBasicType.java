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
package com.datasqrl.io.schema.flexible.type.basic;

import com.datasqrl.error.ErrorCollector;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.NonNull;

public abstract class SimpleBasicType<J> extends AbstractBasicType<J> {

  protected abstract Class<J> getJavaClass();

  protected abstract Function<String, J> getStringParser();

  @Override
  public TypeConversion<J> conversion() {
    return new Conversion<>(getJavaClass(), getStringParser());
  }

  public static class Conversion<J> implements TypeConversion<J> {

    private final Class<J> clazz;
    protected final Function<String, J> stringParser;
    private final Set<Class> javaClasses;

    public Conversion(@NonNull Class<J> clazz, @NonNull Function<String, J> stringParser) {
      this.clazz = clazz;
      this.stringParser = stringParser;
      this.javaClasses = Collections.singleton(clazz);
    }

    @Override
    public Set<Class> getJavaTypes() {
      return javaClasses;
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      return Optional.empty();
    }

    @Override
    public boolean detectType(String original) {
      try {
        stringParser.apply(original);
        return true;
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public Optional<J> parseDetected(Object original, ErrorCollector errors) {
      if (original instanceof String string) {
        try {
          var result = stringParser.apply(string);
          return Optional.of(result);
        } catch (IllegalArgumentException e) {
          errors.fatal("Could not parse value [%s] to data type [%s]", original, clazz);
        }
        return Optional.empty();
      }
      errors.fatal("Cannot convert [%s]", original);
      return Optional.empty();
    }
  }
}
