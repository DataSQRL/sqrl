package com.datasqrl.function;

import com.datasqrl.functions.SqrlFunctions;
import com.google.common.base.Preconditions;
import java.security.SecureRandom;
import java.util.Base64;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

public class SecureFunctions {

  public static RandomID RANDOM_ID = new RandomID();

  public static class RandomID extends ScalarFunction implements SqrlFunction {

    private static final SecureRandom random = new SecureRandom();
    private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

    public String eval(Integer numBytes) {
      if (numBytes==null) return null;
      Preconditions.checkArgument(numBytes>=0);
      byte[] buffer = new byte[numBytes];
      random.nextBytes(buffer);
      return encoder.encodeToString(buffer);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return SqrlFunctions.basicNullInference(DataTypes.STRING(), DataTypes.INT());
    }

    @Override
    public String getDocumentation() {
      return "Generates a random ID string with the given number of secure random bytes. "
          + "The bytes are base64 encoded so the string length will be longer than the number of bytes";
    }
  }

}