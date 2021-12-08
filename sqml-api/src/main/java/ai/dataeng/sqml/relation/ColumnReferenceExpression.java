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

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.schema2.Type;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.flink.types.Row;

import java.util.Objects;

public class ColumnReferenceExpression
        extends RowExpression
        implements Comparable<ColumnReferenceExpression>
{
    final LogicalPlan.Column column;
    int rowOffset = -1;

    public ColumnReferenceExpression(LogicalPlan.Column column) {
        this.column = column;
    }

    /**
     * The offset gets set when using the expression in a physical plan for expedicated execution of expressions
     * @param rowOffset
     */
    public void setRowOffset(int rowOffset) {
        Preconditions.checkArgument(rowOffset>=0);
        this.rowOffset = rowOffset;
    }

    /**
     * This method gets called when the expression is evaluated in the physical plan execution
     * @param row
     * @return
     */
    public Object evaluate(Row row) {
        Preconditions.checkArgument(row!=null && rowOffset>=0);
        return row.getField(rowOffset);
    }

    public String getName()
    {
        return column.getName().getCanonical();
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return column.getType();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column);
    }

    @Override
    public String toString()
    {
        return getName();
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        //return visitor.visitVariableReference(this, context);
        return null; //TODO
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnReferenceExpression other = (ColumnReferenceExpression) obj;
        return Objects.equals(this.column, other.column);
    }

    @Override
    public int compareTo(ColumnReferenceExpression o)
    {
        int nameComparison = getName().compareTo(o.getName());
        if (nameComparison != 0) {
            return nameComparison;
        }
        //TODO: CompareTo
        return 0;
//        return type.getTypeSignature().toString().compareTo(o.type.getTypeSignature().toString());
    }
}
