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
package ai.dataeng.sqml.metadata;

import static ai.dataeng.sqml.common.StandardErrorCode.FUNCTION_NOT_FOUND;
import static ai.dataeng.sqml.common.type.DoubleType.DOUBLE;
import static ai.dataeng.sqml.common.type.UnknownType.UNKNOWN;
import static ai.dataeng.sqml.function.FunctionKind.SCALAR;
import static java.lang.String.format;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.analyzer.TypeSignatureProvider;
import ai.dataeng.sqml.common.CatalogSchemaName;
import ai.dataeng.sqml.common.OperatorType;
import ai.dataeng.sqml.common.PrestoException;
import ai.dataeng.sqml.common.QualifiedObjectName;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.common.type.TypeManager;
import ai.dataeng.sqml.common.type.TypeSignature;
import ai.dataeng.sqml.common.type.TypeSignatureParameter;
import ai.dataeng.sqml.function.FunctionHandle;
import ai.dataeng.sqml.function.FunctionImplementationType;
import ai.dataeng.sqml.function.FunctionKind;
import ai.dataeng.sqml.function.FunctionMetadata;
import ai.dataeng.sqml.function.FunctionVersion;
import ai.dataeng.sqml.function.Signature;
import ai.dataeng.sqml.function.SqlFunction;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class FunctionAndTypeManager
    implements TypeManager {
  public static final CatalogSchemaName DEFAULT_NAMESPACE = new CatalogSchemaName("presto", "default");

  @Inject
  public FunctionAndTypeManager() {

  }

  public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle) {
    return new FunctionMetadata(
        QualifiedObjectName.valueOf("default.a.fun"),
        Optional.empty(),
        List.of(UNKNOWN.getTypeSignature()),
        Optional.of(List.of()),
        UNKNOWN.getTypeSignature(),
        FunctionKind.AGGREGATE,
        FunctionImplementationType.BUILTIN,
        true,
        true,
        FunctionVersion.notVersioned()
    );
  }

  @Override
  public Type getType(TypeSignature signature) {

    return UNKNOWN;
  }

  @Override
  public Type getParameterizedType(String baseTypeName,
      List<TypeSignatureParameter> typeParameters) {
    return getType(new TypeSignature(baseTypeName, typeParameters));
  }

  @Override
  public boolean canCoerce(Type actualType, Type expectedType) {
    return true;
//    return typeCoercer.canCoerce(actualType, expectedType);
  }


  public Collection<SqlFunction> listBuiltInFunctions() {
    return null;
//    return builtInTypeAndFunctionNamespaceManager.listFunctions();
  }

  public Collection<? extends SqlFunction> getFunctions(Session session,
      QualifiedObjectName functionName) {
//    if (functionName.getCatalogSchemaName().equals(DEFAULT_NAMESPACE) &&
//        SessionFunctionUtils.listFunctionNames(session.getSessionFunctions())
//            .contains(functionName.getObjectName())) {
//      return SessionFunctionUtils.getFunctions(session.getSessionFunctions(), functionName);
//    }
//
//    Optional<FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManager = getServingFunctionNamespaceManager(
//        functionName.getCatalogSchemaName());
//    if (!functionNamespaceManager.isPresent()) {
//      throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName));
//    }
//
//    Optional<FunctionNamespaceTransactionHandle> transactionHandle = session.getTransactionId().map(
//        id -> transactionManager
//            .getFunctionNamespaceTransaction(id, functionName.getCatalogName()));
//    return functionNamespaceManager.get().getFunctions(transactionHandle, functionName);
    return null;
  }

  /**
   * Resolves a function using implicit type coercions. We enforce explicit naming for dynamic
   * function namespaces. All unqualified function names will only be resolved against the built-in
   * static function namespace. While it is possible to define an ordering (through SQL path or
   * other means) and convention (best match / first match), in reality when complicated namespaces
   * are involved such implicit resolution might hide errors and cause confusion.
   *
   * @throws PrestoException if there are no matches or multiple matches
   */
  public FunctionHandle resolveFunction(
      QualifiedName functionName,
      List<TypeSignatureProvider> parameterTypes) {
    TypeSignature argumentType = UNKNOWN.getTypeSignature();

    Signature signature = new Signature(
        QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "fun"),
        SCALAR,
        UNKNOWN.getTypeSignature(),
        argumentType);

    return new BuiltInFunctionHandle(signature);
//
//    if (functionName.getCatalogSchemaName().equals(DEFAULT_NAMESPACE)) {
//      if (sessionFunctions.isPresent()) {
//        Collection<SqlFunction> candidates = SessionFunctionUtils
//            .getFunctions(sessionFunctions.get(), functionName);
//        Optional<Signature> match = functionSignatureMatcher
//            .match(candidates, parameterTypes, true);
//        if (match.isPresent()) {
//          return SessionFunctionUtils.getFunctionHandle(sessionFunctions.get(), match.get());
//        }
//      }
//
//
//    }

  }


  public Optional<Type> getCommonSuperType(Type firstType, Type secondType) {
    return null;//typeCoercer.getCommonSuperType(firstType, secondType);
  }

  public boolean isTypeOnlyCoercion(Type actualType, Type expectedType) {
    return false;//typeCoercer.isTypeOnlyCoercion(actualType, expectedType);
  }

  public FunctionHandle resolveOperator(OperatorType operatorType,
      List<TypeSignatureProvider> argumentTypes) {
    return null;
//    try {
//      return resolveFunction(Optional.empty(), Optional.empty(), operatorType.getFunctionName(),
//          argumentTypes);
//    } catch (PrestoException e) {
//      if (e.getErrorCode().getCode() == FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
//        throw new OperatorNotFoundException(
//            operatorType,
//            argumentTypes.stream()
//                .map(TypeSignatureProvider::getTypeSignature)
//                .collect(toImmutableList()));
//      } else {
//        throw e;
//      }
//    }
  }

  public FunctionHandle lookupFunction(String name, List<TypeSignatureProvider> parameterTypes) {
    return null;
//
//    QualifiedObjectName functionName = qualifyObjectName(QualifiedName.of(name));
//    if (parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
//      return lookupCachedFunction(functionName, parameterTypes);
//    }
//
//    Collection<? extends SqlFunction> candidates = builtInTypeAndFunctionNamespaceManager
//        .getFunctions(Optional.empty(), functionName);
//    Optional<Signature> match = functionSignatureMatcher.match(candidates, parameterTypes, false);
//    if (!match.isPresent()) {
//      throw new PrestoException(FUNCTION_NOT_FOUND,
//          constructFunctionNotFoundErrorMessage(functionName, parameterTypes, candidates));
//    }
//
//    return builtInTypeAndFunctionNamespaceManager.getFunctionHandle(Optional.empty(), match.get());
  }

  private FunctionHandle resolveFunctionInternal(
      QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes) {

    throw new PrestoException(FUNCTION_NOT_FOUND,
        functionName + " not found");
  }
}
