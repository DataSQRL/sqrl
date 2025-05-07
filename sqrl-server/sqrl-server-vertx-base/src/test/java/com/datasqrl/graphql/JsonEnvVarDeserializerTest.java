package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

class JsonEnvVarDeserializerTest {


  @Test
  public void testDeserialization() {
    var jsonEnvVarDeserializer = new JsonEnvVarDeserializer();
    var s = jsonEnvVarDeserializer.replaceWithEnv(Map.of("PGPASSWORD",
        "G:eXB4-(70b~$afas%8#.riC3fs1H"), "${PGPASSWORD}");

    assertEquals("G:eXB4-(70b~$afas%8#.riC3fs1H", s);
  }
}