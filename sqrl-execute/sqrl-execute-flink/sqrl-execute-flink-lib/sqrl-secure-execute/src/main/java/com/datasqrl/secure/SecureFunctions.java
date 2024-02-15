package com.datasqrl.secure;

import com.datasqrl.function.FlinkTypeUtil;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

public class SecureFunctions {

  public static RandomID RANDOM_ID = new RandomID();
  public static Uuid UUID = new Uuid();

}
