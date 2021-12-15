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

import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlan.Column;
import ai.dataeng.sqml.relation.CallExpression;
import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.relation.RowExpression;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.Type;

import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.BigIntegerType;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.SymbolReference;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.primitives.Ints;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PlanVariableAllocator
        implements VariableAllocator
{
    private static final Pattern DISALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9_\\-$]+");

    private final Map<String, Type> variables;
    private int nextId;

    public PlanVariableAllocator()
    {
        variables = new HashMap<>();
    }

//    public PlanVariableAllocator(Collection<ColumnReferenceExpression> initial)
//    {
//        this.variables = requireNonNull(initial, "initial is null").stream()
//                .collect(Collectors.toMap(ColumnReferenceExpression::getName, ColumnReferenceExpression::getType));
//    }

//    public ColumnReferenceExpression newVariable(ColumnReferenceExpression variableHint)
//    {
//        checkArgument(variables.containsKey(variableHint.getName()), "variableHint name not in variables map");
//        return newVariable(variableHint.getName(), variableHint.getType());
//    }
//
//    public ColumnReferenceExpression newVariable(QualifiedName nameHint, Type type)
//    {
//        return newVariable(nameHint.getSuffix().getCanonical(), type, null);
//    }

    public ColumnReferenceExpression newVariable(String nameHint, Type type)
    {
        return newVariable(nameHint, type, null);
    }
//
//    public ColumnReferenceExpression newHashVariable()
//    {
//        return newVariable("$hashValue", BigIntegerType.INSTANCE);
//    }

    @Override
    public ColumnReferenceExpression newVariable(String nameHint, Type type, String suffix)
    {
        requireNonNull(nameHint, "name is null");
        requireNonNull(type, "type is null");

        // TODO: workaround for the fact that QualifiedName lowercases parts
        nameHint = nameHint.toLowerCase(ENGLISH);

        // don't strip the tail if the only _ is the first character
        int index = nameHint.lastIndexOf("_");
        if (index > 0) {
            String tail = nameHint.substring(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (Ints.tryParse(tail) != null || index == nameHint.length() - 1) {
                nameHint = nameHint.substring(0, index);
            }
        }

        String unique = nameHint;

        if (suffix != null) {
            unique = unique + "$" + suffix;
        }
        // remove special characters for other special serde
        unique = DISALLOWED_CHAR_PATTERN.matcher(unique).replaceAll("_");

        String attempt = unique;
        while (variables.containsKey(attempt)) {
            attempt = unique + "_" + nextId();
        }

        variables.put(attempt, type);
        return new ColumnReferenceExpression(new Column(Name.system(nameHint),
            null, 0, (BasicType) unbox(type), 0, List.of(), false, false
            ));
    }

    public ColumnReferenceExpression newVariable(Expression expression, Type type)
    {
        return newVariable(expression, type, null);
    }

    public ColumnReferenceExpression newVariable(Expression expression, Type type, String suffix)
    {
        String nameHint = "expr";
        if (expression instanceof Identifier) {
            nameHint = ((Identifier) expression).getValue();
        }
        else if (expression instanceof FunctionCall) {
            nameHint = ((FunctionCall) expression).getName().getSuffix().getCanonical();
        }
        else if (expression instanceof SymbolReference) {
            nameHint = ((SymbolReference) expression).getName();
        }
        else if (expression instanceof GroupingOperation) {
            nameHint = "grouping";
        }
        return newVariable(nameHint, type, suffix);
    }

//    public ColumnReferenceExpression newVariable(Field field)
//    {
//        return newVariable(field.getName().getCanonical(), field.getType(), null);
//    }

//    public TypeProvider getTypes()
//    {
//        return TypeProvider.viewOf(variables);
//    }

    private int nextId()
    {
        return nextId++;
    }

    public ColumnReferenceExpression toVariableReference(Expression expression)
    {
        checkArgument(expression instanceof SymbolReference, "Unexpected expression: %s", expression);
        String name = ((SymbolReference) expression).getName();
        checkArgument(variables.containsKey(name), "variable map does not contain name %s", name);
//        return new ColumnReferenceExpression(name, variables.get(name));
        return new ColumnReferenceExpression(new Column(Name.system(name),
        null, 0, (BasicType) variables.get(name), 0, List.of(), false, false));
    }
//
//    public ColumnReferenceExpression newVariable(RowExpression expression)
//    {
//        return newVariable(expression, null);
//    }
//
//    public ColumnReferenceExpression newVariable(RowExpression expression, String suffix)
//    {
//        String nameHint = "expr";
//        if (expression instanceof ColumnReferenceExpression) {
//            nameHint = ((ColumnReferenceExpression) expression).getName();
//        }
//        else if (expression instanceof CallExpression) {
//            nameHint = ((CallExpression) expression).getDisplayName();
//        }
//        return newVariable(nameHint, expression.getType(), suffix);
//    }
}
