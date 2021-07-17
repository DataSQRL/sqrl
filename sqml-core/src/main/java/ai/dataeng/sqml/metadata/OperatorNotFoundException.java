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

import static ai.dataeng.sqml.common.StandardErrorCode.OPERATOR_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.common.OperatorType;
import ai.dataeng.sqml.common.PrestoException;
import ai.dataeng.sqml.common.type.TypeSignature;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

public class OperatorNotFoundException
        extends PrestoException
{
    private final OperatorType operatorType;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;

    public OperatorNotFoundException(OperatorType operatorType, List<TypeSignature> argumentTypes)
    {
        super(OPERATOR_NOT_FOUND, formatErrorMessage(operatorType, argumentTypes, Optional.empty()));
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.returnType = null;
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    public OperatorNotFoundException(OperatorType operatorType, List<TypeSignature> argumentTypes, TypeSignature returnType)
    {
        super(OPERATOR_NOT_FOUND, formatErrorMessage(operatorType, argumentTypes, Optional.of(returnType)));
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.returnType = requireNonNull(returnType, "returnType is null");
    }

    private static String formatErrorMessage(OperatorType operatorType, List<TypeSignature> argumentTypes, Optional<TypeSignature> returnType)
    {
        String operatorString;
        switch (operatorType) {
            case BETWEEN:
                return format("Cannot check if %s is BETWEEN %s and %s", argumentTypes.get(0), argumentTypes.get(1), argumentTypes.get(2));
            case CAST:
                operatorString = format("%s%s", operatorType.getOperator(), returnType.map(value -> " to " + value).orElse(""));
                break;
            default:
                operatorString = format("'%s'%s", operatorType.getOperator(), returnType.map(value -> ":" + value).orElse(""));
        }
        return format("%s cannot be applied to %s", operatorString, Joiner.on(", ").join(argumentTypes));
    }

    public OperatorType getOperatorType()
    {
        return operatorType;
    }

    public TypeSignature getReturnType()
    {
        return returnType;
    }

    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }
}
