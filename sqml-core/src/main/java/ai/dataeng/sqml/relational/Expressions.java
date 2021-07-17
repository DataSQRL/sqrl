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
package ai.dataeng.sqml.relational;

import static ai.dataeng.sqml.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.collect.ImmutableList.toImmutableList;

import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.function.FunctionHandle;
import ai.dataeng.sqml.metadata.FunctionAndTypeManager;
import ai.dataeng.sqml.relation.CallExpression;
import ai.dataeng.sqml.relation.ConstantExpression;
import ai.dataeng.sqml.relation.RowExpression;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import java.util.List;

public final class Expressions {

  private Expressions() {
  }

  public static ConstantExpression constant(Object value, Type type) {
    return new ConstantExpression(value, type);
  }

  public static CallExpression call(String displayName, FunctionHandle functionHandle,
      Type returnType, List<RowExpression> arguments) {
    return new CallExpression(displayName, functionHandle, returnType, arguments);
  }

  public static CallExpression call(FunctionAndTypeManager functionAndTypeManager, String name,
      Type returnType, List<RowExpression> arguments) {
    FunctionHandle functionHandle = functionAndTypeManager.lookupFunction(name,
        fromTypes(arguments.stream().map(RowExpression::getType).collect(toImmutableList())));
    return call(name, functionHandle, returnType, arguments);
  }

  public static VariableReferenceExpression variable(String name, Type type) {
    return new VariableReferenceExpression(name, type);
  }
}
