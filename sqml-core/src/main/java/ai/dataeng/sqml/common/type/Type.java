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

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.List;

public interface Type {

  /**
   * Gets the name of this type which must be case insensitive globally unique. The name of a user
   * defined type must be a legal identifier in Presto.
   */
  @JsonValue
  TypeSignature getTypeSignature();

  /**
   * Returns the name of this type that should be displayed to end-users.
   */
  String getDisplayName();

  /**
   * True if the type supports equalTo and hash.
   */
  boolean isComparable();

  /**
   * True if the type supports compareTo.
   */
  boolean isOrderable();

  /**
   * Gets the Java class type used to represent this value on the stack during expression
   * execution.
   * <p>
   * Currently, this must be boolean, long, double, Slice or Block.
   */
  Class<?> getJavaType();

  /**
   * For parameterized types returns the list of parameters.
   */
  List<Type> getTypeParameters();
}
