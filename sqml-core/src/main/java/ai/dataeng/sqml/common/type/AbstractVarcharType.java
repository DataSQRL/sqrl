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

import ai.dataeng.sqml.function.SqlFunctionProperties;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.Objects;

public class AbstractVarcharType
    extends AbstractVariableWidthType {

  public static final int UNBOUNDED_LENGTH = Integer.MAX_VALUE;
  public static final int MAX_LENGTH = Integer.MAX_VALUE - 1;

  private final int length;

  AbstractVarcharType(int length, TypeSignature typeSignature) {
    super(typeSignature, Slice.class);

    if (length < 0) {
      throw new IllegalArgumentException("Invalid VARCHAR length " + length);
    }
    this.length = length;
  }

  @Deprecated
  public int getLength() {
    return length;
  }

  public int getLengthSafe() {
    if (isUnbounded()) {
      throw new IllegalStateException("Cannot get size of unbounded VARCHAR.");
    }
    return length;
  }

  public boolean isUnbounded() {
    return length == UNBOUNDED_LENGTH;
  }

  @Override
  public boolean isComparable() {
    return true;
  }

  @Override
  public boolean isOrderable() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractVarcharType other = (AbstractVarcharType) o;

    return Objects.equals(this.length, other.length);
  }

  @Override
  public int hashCode() {
    return Objects.hash(length);
  }

  @Override
  public String getDisplayName() {
    if (length == UNBOUNDED_LENGTH) {
      return getTypeSignature().getBase();
    }

    return getTypeSignature().toString();
  }

  @Override
  public String toString() {
    return getDisplayName();
  }
}
