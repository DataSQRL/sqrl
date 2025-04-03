package com.datasqrl.datatype;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@AutoService(AutoRegisterSystemFunction.class)
public class HashColumns extends ScalarFunction implements AutoRegisterSystemFunction{

  public String eval(Object... objects) {
    if (objects.length==0) return "";
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");  // Changed to MD5
      for (Object obj : objects) {
        int hash = Objects.hashCode(obj); //to handle null objects
        digest.update(Integer.toString(hash).getBytes(StandardCharsets.UTF_8));
      }

      byte[] hashBytes = digest.digest();
      StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
      for (byte b : hashBytes) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    InputTypeStrategy inputTypeStrategy = InputTypeStrategies.compositeSequence()
        .finishWithVarying(InputTypeStrategies.WILDCARD);

    return TypeInference.newBuilder().inputTypeStrategy(inputTypeStrategy).outputTypeStrategy(
        TypeStrategies.explicit(DataTypes.CHAR(32))).build();
  }
}