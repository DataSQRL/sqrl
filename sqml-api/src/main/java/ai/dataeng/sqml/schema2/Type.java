package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.io.Serializable;

public interface Type extends Serializable {

  default boolean isOrderable() {
    return true;
  }

  default boolean isComparable() {
    return true;
  }

  default public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitType(this, context);
  }
}
