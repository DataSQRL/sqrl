package com.datasqrl.plan.global;

import com.google.common.base.Preconditions;

public enum MaterializationPreference {

  MUST, SHOULD, SHOULD_NOT, CANNOT;

  public boolean isMaterialize() {
    switch (this) {
      case MUST:
      case SHOULD:
        return true;
      default:
        return false;
    }
  }

  public boolean canMaterialize() {
    return this != CANNOT;
  }

  public boolean isCompatible(MaterializationPreference other) {
    if ((this == MUST && other == CANNOT) || (this == CANNOT && other == MUST)) {
      return false;
    }
    return true;
  }

  public MaterializationPreference combine(MaterializationPreference other) {
    Preconditions.checkArgument(isCompatible(other),
        "Materialization preferences are not compatible");
    if (this == MUST || other == MUST) {
      return MUST;
    }
    if (this == CANNOT || other == CANNOT) {
      return CANNOT;
    }
    if (this == SHOULD || other == SHOULD) {
      return SHOULD;
    }
    return SHOULD_NOT;
  }

}
