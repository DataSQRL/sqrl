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
package ai.dataeng.sqml.planner.operator.relation;

import ai.dataeng.sqml.type.Type;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class CallExpression
        extends RowExpression
{
    String displayName;
    Type returnType;
    List<RowExpression> arguments;

    public String getDisplayName()
    {
        return displayName;
    }

    public Type getType()
    {
        return returnType;
    }

    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString() {
        return displayName + "(" + String.join(", ", arguments.stream().map(RowExpression::toString).collect(Collectors.toList())) + ")";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arguments);
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
        CallExpression other = (CallExpression) obj;
        return Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }
}
