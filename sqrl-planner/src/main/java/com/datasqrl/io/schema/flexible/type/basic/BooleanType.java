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

import com.datasqrl.io.schema.flexible.type.SqrlTypeVisitor;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class BooleanType extends AbstractBasicType<Boolean> {

  private static final Function<String, Boolean> parseBoolean =
      s -> {
        if (s.equalsIgnoreCase("true")) {
          return true;
        } else if (s.equalsIgnoreCase("false")) {
          return false;
        }
        throw new IllegalArgumentException("Not a boolean");
      };

  public static final BooleanType INSTANCE = new BooleanType();

  @Override
  public List<String> getNames() {
    return List.of("BOOLEAN");
  }

  @Override
  public Conversion conversion() {
    return Conversion.INSTANCE;
  }

  private static class Conversion extends SimpleBasicType.Conversion<Boolean> {

    private static final Conversion INSTANCE = new Conversion();

    private Conversion() {
      super(Boolean.class, parseBoolean);
    }

    @Override
    public Boolean convert(Object o) {
      if (o instanceof Boolean boolean1) {
        return boolean1;
      }
      if (o instanceof Number number) {
        return number.longValue() > 0;
      }
      throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      if (fromType instanceof BigIntType) {
        return Optional.of(80);
      }
      return Optional.empty();
    }
  }

  @Override
  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitBooleanType(this, context);
  }
}
