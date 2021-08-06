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
package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.ArithmeticUnaryExpression;
import ai.dataeng.sqml.tree.ArrayConstructor;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.AtTimeZone;
import ai.dataeng.sqml.tree.BetweenPredicate;
import ai.dataeng.sqml.tree.Cast;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.DereferenceExpression;
import ai.dataeng.sqml.tree.ExistsPredicate;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.InListExpression;
import ai.dataeng.sqml.tree.InPredicate;
import ai.dataeng.sqml.tree.IsNotNullPredicate;
import ai.dataeng.sqml.tree.IsNullPredicate;
import ai.dataeng.sqml.tree.LikePredicate;
import ai.dataeng.sqml.tree.Literal;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.NotExpression;
import ai.dataeng.sqml.tree.Parameter;
import ai.dataeng.sqml.tree.Row;
import ai.dataeng.sqml.tree.SimpleCaseExpression;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.SubqueryExpression;
import ai.dataeng.sqml.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Checks whether an expression is constant with respect to the group
 */
class AggregationAnalyzer
{
    public static void verifySourceAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Expression expression,
            Metadata metadata,
            Analysis analysis) {
        //todo
    }

    public static void verifyOrderByAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Scope orderByScope,
            Expression expression,
            Metadata metadata,
            Analysis analysis) {
        //todo
    }
}
