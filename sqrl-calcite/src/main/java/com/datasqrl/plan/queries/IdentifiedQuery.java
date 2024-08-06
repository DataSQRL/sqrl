package com.datasqrl.plan.queries;

import lombok.Value;

public interface IdentifiedQuery {

  String getNameId();

  static IdentifiedQuery of(String nameId) {
    return new Instance(nameId);
  }

  @Value
  class Instance implements IdentifiedQuery {
    String nameId;
  }


}
