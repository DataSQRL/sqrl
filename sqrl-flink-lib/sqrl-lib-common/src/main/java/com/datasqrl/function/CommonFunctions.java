package com.datasqrl.function;

import com.datasqrl.datatype.HashColumns;
import com.datasqrl.datatype.Noop;
import com.datasqrl.datatype.SerializeToBytes;

public class CommonFunctions {

  public static final SerializeToBytes SERIALIZE_TO_BYTES = new SerializeToBytes();
  public static final Noop NOOP = new Noop();
  public static final HashColumns HASH_COLUMNS = new HashColumns();

}
