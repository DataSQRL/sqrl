package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonEnvVarDeserializerTest {

  @Test
  public void testDeserialization() {
    JsonEnvVarDeserializer jsonEnvVarDeserializer = new JsonEnvVarDeserializer();
    String s =
        jsonEnvVarDeserializer.replaceWithEnv(
            Map.of("PGPASSWORD", "G:eXB4-(70b~$afas%8#.riC3fs1H"), "${PGPASSWORD}");

    assertEquals("G:eXB4-(70b~$afas%8#.riC3fs1H", s);
  }
}
