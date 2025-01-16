package com.datasqrl.flinkwrapper.parser;

public enum AccessModifier {

  QUERY, //Maps to a query
  SUBSCRIPTION, //Maps to a subscription
  HIDDEN, //Hidden, no access
  INHERIT //Inherits access modifier from parent (for stacked statements)

}
