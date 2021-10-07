package ai.dataeng.sqml.schema2;

import java.io.Serializable;

public interface Type extends Serializable {

  default boolean isOrderable() {
    return true;
  }

  default boolean isComparable() {
    return true;
  }
}
