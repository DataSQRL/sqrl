package com.datasqrl.graphql.inference;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import lombok.SneakyThrows;

public class CipherUtil {

  @SneakyThrows
  public static String sha256(String input) {
    MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

    BigInteger bigInteger = new BigInteger(1, messageDigest
        .digest(input.getBytes(StandardCharsets.UTF_8)));
    String calculatedChecksum = String.format("%064x", bigInteger);
    return calculatedChecksum;
  }
}
