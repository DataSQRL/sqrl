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

import static ai.dataeng.sqml.common.type.TypeSignature.parseTypeSignature;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;

import ai.dataeng.sqml.function.SqlFunctionProperties;

public final class DoubleType
    extends AbstractType
    implements FixedWidthType {

  public static final DoubleType DOUBLE = new DoubleType();

  private DoubleType() {
    super(parseTypeSignature(StandardTypes.DOUBLE), double.class);
  }

  @Override
  public final int getFixedSize() {
    return Double.BYTES;
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
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  public boolean equals(Object other) {
    return other == DOUBLE;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }
}
