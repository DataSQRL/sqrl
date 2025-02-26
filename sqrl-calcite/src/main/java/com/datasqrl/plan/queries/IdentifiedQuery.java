package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;
import java.util.Optional;

public interface IdentifiedQuery {

  String getNameId();

  /**
   * @return If this query defines a view, it returns the name of the underlying table that defined
   *     it, * else it returns empty
   */
  public Optional<Name> getViewName();
}
