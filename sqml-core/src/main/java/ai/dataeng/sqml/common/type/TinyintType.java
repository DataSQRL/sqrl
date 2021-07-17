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
import static java.lang.Long.rotateLeft;
import static java.lang.String.format;

public final class TinyintType
    extends AbstractType
    implements FixedWidthType {

  public static final TinyintType TINYINT = new TinyintType();

  private TinyintType() {
    super(parseTypeSignature(StandardTypes.TINYINT), long.class);
  }

  public static long hash(byte value) {
    // xxhash64 mix
    return rotateLeft(value * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
  }

  @Override
  public int getFixedSize() {
    return Byte.BYTES;
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
  public boolean equals(Object other) {
    return other == TINYINT;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }
}
