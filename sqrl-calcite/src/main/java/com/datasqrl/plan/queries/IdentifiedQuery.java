package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.NamePath;
import lombok.Value;

public interface IdentifiedQuery {

  String getNameId();

  NamePath getNamePath();

}
