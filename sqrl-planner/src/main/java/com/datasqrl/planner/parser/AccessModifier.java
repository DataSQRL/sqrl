package com.datasqrl.planner.parser;

/**
 * The defined (or implied) access for a definition
 */
public enum AccessModifier {

  NONE, //no access
  QUERY, //Maps to a query
  SUBSCRIPTION, //Maps to a subscription
  INHERIT //Inherits access modifier from parent (for stacked statements)

}
