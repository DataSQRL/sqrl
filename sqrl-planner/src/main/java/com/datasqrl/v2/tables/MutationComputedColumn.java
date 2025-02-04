package com.datasqrl.v2.tables;

import lombok.Value;

/**
 * The name of the uuid and timestamp column (if any) as defined in the CREATE TABLE statement.
 *
 * TODO: We want to generalize this to allow arbitrary upfront computations
 * on the server and not have this hardcoded to just these two columns.
 */
@Value
public class MutationComputedColumn {

  public static final String UUID_COLUMN = "_uuid";
  public static final String TIMESTAMP_METADATA = "timestamp";

  public enum Type { UUID, TIMESTAMP }

  String columnName;
  Type type;


}
