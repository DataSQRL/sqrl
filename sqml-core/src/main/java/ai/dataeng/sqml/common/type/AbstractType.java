/*
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
package ai.dataeng.sqml.common.type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractType
    implements Type {

  private final TypeSignature signature;
  private final Class<?> javaType;

  protected AbstractType(TypeSignature signature,
      Class<?> javaType) {
    this.signature = signature;
    this.javaType = javaType;
  }

  @Override
  public final TypeSignature getTypeSignature() {
    return signature;
  }

  @Override
  public String getDisplayName() {
    return signature.toString();
  }

  @Override
  public final Class<?> getJavaType() {
    return javaType;
  }

  @Override
  public List<Type> getTypeParameters() {
    return Collections.unmodifiableList(new ArrayList<>());
  }

  @Override
  public boolean isComparable() {
    return false;
  }

  @Override
  public boolean isOrderable() {
    return false;
  }


  @Override
  public String toString() {
    return getTypeSignature().toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return this.getTypeSignature()
        .equals(((Type) o).getTypeSignature());
  }

  @Override
  public int hashCode() {
    return signature.hashCode();
  }
}
