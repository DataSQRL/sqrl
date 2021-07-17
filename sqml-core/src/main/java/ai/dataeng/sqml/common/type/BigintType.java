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

import ai.dataeng.sqml.function.SqlFunctionProperties;

public final class BigintType
    extends AbstractLongType {

  public static final BigintType BIGINT = new BigintType();

  private BigintType() {
    super(parseTypeSignature(StandardTypes.BIGINT));
  }


  @Override
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  public boolean equals(Object other) {
    return other == BIGINT;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }
}
