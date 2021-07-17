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

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;

public class FunctionType
    implements Type {

  public static final String NAME = "function";

  private final TypeSignature signature;
  private final Type returnType;
  private final List<Type> argumentTypes;

  public FunctionType(List<Type> argumentTypes, Type returnType) {
    this.signature = new TypeSignature(NAME, typeParameters(argumentTypes, returnType));
    this.returnType = requireNonNull(returnType, "returnType is null");
    this.argumentTypes = unmodifiableList(
        new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null")));
  }

  private static List<TypeSignatureParameter> typeParameters(List<Type> argumentTypes,
      Type returnType) {
    requireNonNull(returnType, "returnType is null");
    requireNonNull(argumentTypes, "argumentTypes is null");
    List<TypeSignatureParameter> parameters = new ArrayList<>(argumentTypes.size() + 1);
    argumentTypes.stream()
        .map(Type::getTypeSignature)
        .map(TypeSignatureParameter::of)
        .forEach(parameters::add);
    parameters.add(TypeSignatureParameter.of(returnType.getTypeSignature()));
    return unmodifiableList(parameters);
  }

  public Type getReturnType() {
    return returnType;
  }

  public List<Type> getArgumentTypes() {
    return argumentTypes;
  }

  @Override
  public List<Type> getTypeParameters() {
    List<Type> parameters = new ArrayList<>(argumentTypes.size() + 1);
    parameters.addAll(argumentTypes);
    parameters.add(returnType);
    return unmodifiableList(parameters);
  }

  @Override
  public final TypeSignature getTypeSignature() {
    return signature;
  }

  @Override
  public String getDisplayName() {
    List<String> names = getTypeParameters().stream()
        .map(Type::getDisplayName)
        .collect(toList());
    return "function<" + String.join(",", names) + ">";
  }

  @Override
  public final Class<?> getJavaType() {
    throw new UnsupportedOperationException(getTypeSignature() + " type does not have Java type");
  }

  @Override
  public boolean isComparable() {
    return false;
  }

  @Override
  public boolean isOrderable() {
    return false;
  }

}
