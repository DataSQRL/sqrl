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

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.common.QualifiedObjectName;
import java.util.List;
import java.util.Objects;

public class TypeWithName
    implements Type {

  private final QualifiedObjectName name;
  private final Type type;
  private final TypeSignature typeSignature;

  public TypeWithName(QualifiedObjectName name, Type type) {
    this.name = requireNonNull(name, "name is null");
    this.type = requireNonNull(type, "type is null");
    this.typeSignature = new TypeSignature(new UserDefinedType(name, type.getTypeSignature()));
  }

  @Override
  public TypeSignature getTypeSignature() {
    return typeSignature;
  }

  @Override
  public String getDisplayName() {
    return name.toString();
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return typeSignature.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    TypeWithName other = (TypeWithName) obj;

    return Objects.equals(this.name, other.name) &&
        Objects.equals(this.type, other.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  // All aspect related to execution are delegated to type
  @Override
  public boolean isComparable() {
    return type.isComparable();
  }

  @Override
  public boolean isOrderable() {
    return type.isOrderable();
  }

  @Override
  public Class<?> getJavaType() {
    return type.getJavaType();
  }

  @Override
  public List<Type> getTypeParameters() {
    return type.getTypeParameters();
  }
}
