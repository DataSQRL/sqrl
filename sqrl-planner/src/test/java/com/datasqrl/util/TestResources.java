package com.datasqrl.util;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TestResources {

  public static final Path RESOURCE_DIR = Paths.get("src", "test", "resources");
  public static final Path CONFIG_YML = RESOURCE_DIR.resolve("simple-config.yml");

}
