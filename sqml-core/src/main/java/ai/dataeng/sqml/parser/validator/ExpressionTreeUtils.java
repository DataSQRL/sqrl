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
package ai.dataeng.sqml.parser.validator;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.tree.DefaultExpressionTraversalVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Node;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Predicate;

public final class ExpressionTreeUtils
{
    private ExpressionTreeUtils() {}

    public static List<FunctionCall> extractAggregateFunctions(Iterable<? extends Node> nodes) {
        return extractExpressions(nodes, FunctionCall.class, isAggregationPredicate());
    }
    public static Predicate<FunctionCall> isAggregationPredicate() {


        return functionCall -> {
            throw new RuntimeException("Need to implement function resolution");
//            Optional<FunctionDefinition> function = functionProvider.resolve(functionCall.getName());
//            return function.orElseThrow(()->
//                    new RuntimeException(String.format("Could not find function %s", functionCall.getName())))
//                .getKind() == FunctionKind.AGGREGATE;
        };
    }

    public static <T extends Expression> List<T> extractExpressions(
        Iterable<? extends Node> nodes,
        Class<T> clazz)
    {
        return extractExpressions(nodes, clazz, alwaysTrue());
    }

    public static <T extends Expression> List<T> extractExpressions(
        Iterable<? extends Node> nodes,
        Class<T> clazz,
        Predicate<T> predicate)
    {
        requireNonNull(nodes, "nodes is null");
        requireNonNull(clazz, "clazz is null");
        requireNonNull(predicate, "predicate is null");

        return ImmutableList.copyOf(nodes).stream()
            .flatMap(node -> linearizeNodes(node).stream())
            .filter(clazz::isInstance)
            .map(clazz::cast)
            .filter(predicate)
            .collect(toImmutableList());
    }

    public static List<Node> linearizeNodes(Node node)
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Node, Void>()
        {
            @Override
            public Node process(Node node, Void context)
            {
                Node result = super.process(node, context);
                nodes.add(node);
                return result;
            }
        }.process(node, null);
        return nodes.build();
    }
}
