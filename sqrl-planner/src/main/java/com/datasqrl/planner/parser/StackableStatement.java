package com.datasqrl.planner.parser;

/**
 * Marker interface for {@link SqrlStatement} that can be stacked on top of each other
 */
public interface StackableStatement extends SqrlStatement {

  boolean isRoot();

}
