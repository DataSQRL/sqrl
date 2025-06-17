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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class StringType extends AbstractBasicType<String> {

  public static final StringType INSTANCE = new StringType();

  @Override
  public List<String> getNames() {
    return List.of("STRING");
  }

  @Override
  public TypeConversion<String> conversion() {
    return new Conversion();
  }

  public static class Conversion implements TypeConversion<String> {

    public Conversion() {}

    @Override
    public Set<Class> getJavaTypes() {
      return Collections.singleton(String.class);
    }

    @Override
    public String convert(Object o) {
      return o.toString();
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      return Optional.of(30);
    }
  }

  @Override
  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitStringType(this, context);
  }
}
