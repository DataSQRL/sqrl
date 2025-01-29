package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.flinkwrapper.parser.AccessModifier;
import lombok.Value;

/**
 * Defines the visibility of a {@link com.datasqrl.flinkwrapper.tables.SqrlTableFunction}
 */
@Value
public class AccessVisibility {

  public static final AccessVisibility NONE = new AccessVisibility(AccessModifier.NONE, false, false, true);

  /**
   * The type of access: Query or Subscription; if access type is NONE it means
   * the function cannot be queried and is only used for planning.
   */
  AccessModifier access;
  /**
   * If the table has been annotated as a test in the SQRL file
   */
  boolean isTest;
  /**
   * Access only table functions are only queryable and not added
   * to the planner, i.e. they cannot be referenced in subsequent definitions
   */
  boolean isAccessOnly;
  /**
   * Whether this table (function) is visible to external consumers (i.e. as
   * an API call or view)
   */
  boolean isHidden;


  public boolean isFunctionSink() {
    return (access==AccessModifier.QUERY || access==AccessModifier.SUBSCRIPTION);
  }

}
