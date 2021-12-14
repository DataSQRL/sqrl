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

import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.logical4.LogicalPlan.Node;
import ai.dataeng.sqml.logical4.LogicalPlan.RowNode;
import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.schema2.RelationType;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The purpose of this class is to hold the current plan built so far
 * for a relation (query, table, values, etc.), and the mapping to
 * indicate how the fields (by position) in the relation map to
 * the outputs of the plan.
 * <p>
 * Fields are resolved by {@link TranslationMap} within local scopes hierarchy.
 * Indexes of resolved parent scope fields start from "total number of child scope fields".
 * For instance if a child scope has n fields, then first parent scope field
 * will have index n.
 */
public class RelationPlan
{
    private final Node root;
    private final List<ColumnReferenceExpression> fieldMappings; // for each field in the relation, the corresponding variable from "root"
    private final Scope scope;

    public RelationPlan(Node root, Scope scope, List<ColumnReferenceExpression> fieldMappings)
    {
        requireNonNull(root, "root is null");
        requireNonNull(fieldMappings, "outputSymbols is null");
        requireNonNull(scope, "scope is null");

        int allFieldCount = getAllFieldCount(scope);
        checkArgument(allFieldCount == fieldMappings.size(),
                "Number of outputs (%s) doesn't match number of fields in scopes tree (%s)",
                fieldMappings.size(),
                allFieldCount);

        this.root = root;
        this.scope = scope;
        this.fieldMappings = ImmutableList.copyOf(fieldMappings);
    }

    public ColumnReferenceExpression getVariable(int fieldIndex)
    {
        checkArgument(fieldIndex >= 0 && fieldIndex < fieldMappings.size(), "No field->symbol mapping for field %s", fieldIndex);
        return fieldMappings.get(fieldIndex);
    }

    public Node getRoot()
    {
        return root;
    }

    public List<ColumnReferenceExpression> getFieldMappings()
    {
        return fieldMappings;
    }

    public RelationType getDescriptor()
    {
        return scope.getRelation();
    }

    public Scope getScope()
    {
        return scope;
    }

    private static int getAllFieldCount(Scope root)
    {
        int allFieldCount = 0;
        Optional<Scope> current = Optional.of(root);
//        while (current.isPresent()) {
            allFieldCount += current.get().getRelation().getAllFieldCount();
//            current = current.get().getLocalParent();
//        }
        return allFieldCount;
    }
}
