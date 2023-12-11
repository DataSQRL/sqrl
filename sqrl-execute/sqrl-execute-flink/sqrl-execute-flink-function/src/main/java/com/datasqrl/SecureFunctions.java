package com.datasqrl;

import com.datasqrl.function.SqrlFunction;
import com.google.common.base.Preconditions;
import java.math.BigInteger;
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

  public static class RandomID extends ScalarFunction implements SqrlFunction {

    private static final SecureRandom random = new SecureRandom();
    private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

    public String eval(Long numBytes) {
      if (numBytes==null) return null;
      Preconditions.checkArgument(numBytes>=0);
      byte[] buffer = new byte[numBytes.intValue()];
      random.nextBytes(buffer);
      return encoder.encodeToString(buffer);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return SqrlFunctions.basicNullInferenceBuilder(DataTypes.STRING(), DataTypes.BIGINT())
          .typedArguments(List.of(DataTypes.BIGINT()))
          .build();
    }

    @Override
    public String getDocumentation() {
      return "Generates a random ID string with the given number of secure random bytes. "
          + "The bytes are base64 encoded so the string length will be longer than the number of bytes";
    }
  }

  public static class Uuid extends ScalarFunction implements SqrlFunction {

    public String eval() {
      return java.util.UUID.randomUUID().toString();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .typedArguments()
          .outputTypeStrategy(callContext -> Optional.of(DataTypes.CHAR(36).notNull()))
          .build();
    }

    @Override
    public String getDocumentation() {
      return "Generates a random UUID string";
    }
  }

}
