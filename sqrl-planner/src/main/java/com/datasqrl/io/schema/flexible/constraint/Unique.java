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
package com.datasqrl.io.schema.flexible.constraint;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.type.Type;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public class Unique implements Constraint {

  public static final Name NAME = Name.system("unique");

  public static final Unique UNCONSTRAINED = new Unique();

  private Unique() {} // For Kryo

  @Override
  public boolean satisfies(Object value) {
    return true;
  }

  @Override
  public boolean appliesTo(Type type) {
    return false;
  }

  @Override
  public Name getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> export() {
    return Map.of();
  }

  @Override
  public String toString() {
    return NAME.getDisplay();
  }

  public static class Factory implements Constraint.Factory {

    @Override
    public Name getName() {
      return NAME;
    }

    @Override
    public Optional<Constraint> create(Map<String, Object> parameters, ErrorCollector errors) {
      return Optional.of(new Unique());
    }
  }
}
