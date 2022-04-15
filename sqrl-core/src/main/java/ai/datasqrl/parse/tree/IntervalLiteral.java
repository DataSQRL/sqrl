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
package ai.datasqrl.parse.tree;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public class IntervalLiteral
    extends Literal {

  private final Expression expression;
  private final Sign sign;
  private final IntervalField startField;

  public IntervalLiteral(Optional<NodeLocation> location, Expression expression, Sign sign,
      IntervalField startField) {
    super(location);
    requireNonNull(expression, "expression is null");
    requireNonNull(sign, "sign is null");
    requireNonNull(startField, "startField is null");

    this.expression = expression;
    this.sign = sign;
    this.startField = startField;
  }

  public Expression getExpression() {
    return expression;
  }

  public Sign getSign() {
    return sign;
  }

  public IntervalField getStartField() {
    return startField;
  }

  public boolean isYearToMonth() {
    return startField == IntervalField.YEAR || startField == IntervalField.MONTH;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIntervalLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IntervalLiteral that = (IntervalLiteral) o;
    return Objects.equals(expression, that.expression) && sign == that.sign
        && startField == that.startField;
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, sign, startField);
  }

  public enum Sign {
    POSITIVE {
      @Override
      public int multiplier() {
        return 1;
      }
    },
    NEGATIVE {
      @Override
      public int multiplier() {
        return -1;
      }
    };

    public abstract int multiplier();
  }

  public enum IntervalField {
    YEAR, MONTH, DAY, WEEK, HOUR, MINUTE, SECOND
  }
}
