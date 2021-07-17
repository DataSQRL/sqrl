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

import static java.lang.Long.rotateLeft;


public abstract class AbstractIntType
    extends AbstractType
    implements FixedWidthType {

  protected AbstractIntType(TypeSignature signature) {
    super(signature, long.class);
  }

  public static long hash(int value) {
    // xxhash64 mix
    return rotateLeft(value * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
  }

  @Override
  public final int getFixedSize() {
    return Integer.BYTES;
  }

  @Override
  public boolean isComparable() {
    return true;
  }

  @Override
  public boolean isOrderable() {
    return true;
  }
}
