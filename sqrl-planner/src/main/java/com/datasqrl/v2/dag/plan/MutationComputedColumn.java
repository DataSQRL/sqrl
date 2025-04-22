package com.datasqrl.v2.dag.plan;

import com.datasqrl.graphql.server.MutationComputedColumnType;

import lombok.Value;

/**
 * The name of the uuid and timestamp column (if any) as defined in the CREATE TABLE statement.
 *
 * TODO: We want to generalize this to allow arbitrary upfront computations
 * on the server and not have this hardcoded to just these two columns.
 */
@Value
public class MutationComputedColumn {

  public static final String UUID_METADATA = "uuid";
  public static final String TIMESTAMP_METADATA = "timestamp";

  String columnName;
  MutationComputedColumnType type;


}
