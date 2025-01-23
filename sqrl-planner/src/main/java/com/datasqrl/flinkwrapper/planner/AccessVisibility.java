package com.datasqrl.flinkwrapper.planner;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.flinkwrapper.parser.AccessModifier;
import lombok.Value;

@Value
public class AccessVisibility {

  AccessModifier access;
  boolean isTest;
  boolean isWorkload;
  boolean isHidden;

  public static AccessVisibility forSource(Name name) {
    return new AccessVisibility(AccessModifier.QUERY, false, false, name.isHidden());
  }

  public boolean addAccessFunction() {
    return isTest || isWorkload || !isHidden;
  }

}
