package ai.datasqrl.schema.type;

import java.io.Serializable;

public interface Type extends Serializable {

  default boolean isOrderable() {
    return true;
  }

  default boolean isComparable() {
    return true;
  }

  default <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitType(this, context);
  }
}
