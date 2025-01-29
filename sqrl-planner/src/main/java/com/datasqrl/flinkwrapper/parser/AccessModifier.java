package com.datasqrl.flinkwrapper.parser;

public enum AccessModifier {

  NONE, //no access
  QUERY, //Maps to a query
  SUBSCRIPTION, //Maps to a subscription
  INHERIT //Inherits access modifier from parent (for stacked statements)

}
