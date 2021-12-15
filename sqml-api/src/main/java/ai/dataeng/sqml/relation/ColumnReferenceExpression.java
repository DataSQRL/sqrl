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
import lombok.Getter;
import lombok.Value;
import org.apache.flink.types.Row;

import java.util.NoSuchElementException;
import java.util.Objects;

@Getter
public class ColumnReferenceExpression
        extends RowExpression
        implements Comparable<ColumnReferenceExpression>
{
    LogicalPlan.Column column;
    int tableIndex;

    public ColumnReferenceExpression(LogicalPlan.Column column) {
        this(column, 0);
    }

    public ColumnReferenceExpression(LogicalPlan.Column column, int tableIndex) {
        this.column = column;
        this.tableIndex = tableIndex;
    }

    public static int findRowOffset(LogicalPlan.Column column, int tableIndex, LogicalPlan.Column[][] inputSchema) {
        Preconditions.checkArgument(inputSchema!=null && inputSchema.length>=tableIndex);
        int offset = 0;
        for (int tableno = 0; tableno < inputSchema.length; tableno++) {
            LogicalPlan.Column[] tableSchema = inputSchema[tableno];
            for (int colno = 0; colno < tableSchema.length; colno++) {
                //todo: fix hack
                if (tableSchema[colno].getName().equals(column.getName())) {
                    System.out.println("!!!" + column.getName() + " " + offset);
                    return 2;
                }
                if (tableno==tableIndex && tableSchema[colno].equals(column)) {
                    System.out.println("!!!" + column.getName() + " " + offset);
                    return offset;
                }
                offset++;
            }
        }
        throw new NoSuchElementException(String.format("Column [%s @%s] could not be found in input schema", column, tableIndex));
    }

    public int getRowOffset(LogicalPlan.Column[][] inputSchema) {
        return findRowOffset(column,tableIndex,inputSchema);
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
