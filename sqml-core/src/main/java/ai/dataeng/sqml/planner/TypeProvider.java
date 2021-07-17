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
package ai.dataeng.sqml.planner;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import ai.dataeng.sqml.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class TypeProvider {

  private final Map<String, Type> types;

  private TypeProvider(Map<String, Type> types) {
    this.types = types;
  }

  public static TypeProvider viewOf(Map<String, Type> types) {
    return new TypeProvider(types);
  }

  public static TypeProvider empty() {
    return new TypeProvider(ImmutableMap.of());
  }

  public Type get(Expression expression) {
    requireNonNull(expression, "expression is null");
    Symbol symbol = Symbol.from(expression);
    Type type = types.get(symbol.getName());
    checkArgument(type != null, "no type found found for symbol '%s'", symbol);

    return type;
  }
}
