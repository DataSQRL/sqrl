package com.datasqrl.plan.queries;

import java.util.Optional;

import com.datasqrl.canonicalizer.Name;

public interface IdentifiedQuery {

  String getNameId();

  /**
   * @return If this query defines a view, it returns the name of the underlying table that defined it,
   *    * else it returns empty
   */
  public Optional<Name> getViewName();

}
