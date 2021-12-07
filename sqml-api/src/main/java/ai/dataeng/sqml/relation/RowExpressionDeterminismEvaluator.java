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
package ai.dataeng.sqml.relation;

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.function.FunctionAndTypeManager;
import lombok.Value;

@Value
public class RowExpressionDeterminismEvaluator
        implements DeterminismEvaluator
{
    FunctionAndTypeManager functionAndTypeManager;

    public RowExpressionDeterminismEvaluator(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public boolean isDeterministic(RowExpression expression)
    {
        return expression.accept(new Visitor(functionAndTypeManager), null);
    }

    private static class Visitor
            implements RowExpressionVisitor<Boolean, Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        public Visitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = functionAndTypeManager;
        }

        @Override
        public Boolean visitConstant(ConstantExpression literal, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitCall(CallExpression call, Void context)
        {
            //TODO:
            return true;
//            FunctionHandle functionHandle = call.getFunctionHandle();
//            try {
//                if (!functionAndTypeManager.getFunctionMetadata(functionHandle).isDeterministic()) {
//                    return false;
//                }
//            }
//            catch (PrestoException e) {
//                if (e.getErrorCode().getCode() != FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
//                    throw e;
//                }
//            }
//
//            return call.getArguments().stream()
//                    .allMatch(expression -> expression.accept(this, context));
        }

        @Override
        public Boolean visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return specialForm.getArguments().stream()
                    .allMatch(expression -> expression.accept(this, context));
        }
    }
}
