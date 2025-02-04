package com.datasqrl.function;

import org.apache.flink.table.functions.FunctionDefinition;

/**
 * Marker interface for functions that are used by DataSQRL to down- and up-cast
 * types when moving data between engines
 */
public interface SqrlCastFunction extends FunctionDefinition {

}
