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

import static ai.dataeng.sqml.common.type.Decimals.MAX_PRECISION;
import static ai.dataeng.sqml.common.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;

import io.airlift.slice.Slice;

final class LongDecimalType
    extends DecimalType {

  LongDecimalType(int precision, int scale) {
    super(precision, scale, Slice.class);
    validatePrecisionScale(precision, scale, MAX_PRECISION);
  }

  @Override
  public int getFixedSize() {
    return UNSCALED_DECIMAL_128_SLICE_LENGTH;
  }

}
