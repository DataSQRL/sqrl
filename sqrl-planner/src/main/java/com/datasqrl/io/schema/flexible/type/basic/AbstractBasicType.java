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

public abstract class AbstractBasicType<J> implements BasicType<J> {

  AbstractBasicType() {}

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public String getName() {
    return getNames().get(0);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractBasicType<?> that = (AbstractBasicType<?>) o;
    return getName().equals(that.getName());
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public int compareTo(BasicType<?> o) {
    return getName().compareTo(o.getName());
  }

  @Override
  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitBasicType(this, context);
  }
}
