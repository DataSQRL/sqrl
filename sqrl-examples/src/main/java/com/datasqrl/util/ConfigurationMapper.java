package com.datasqrl.util;

import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class ConfigurationMapper {

  public static void readProperties(Configuration targetObject, Path propertiesFile)
      throws FileNotFoundException, IOException, ConfigurationException {
    if (!Files.isRegularFile(propertiesFile)) {
      System.out.println(String.format("Could not find configuration file [%s]. Using default configuration.",
          propertiesFile));
      return;
    }

    Configurations configurations = new Configurations();
    FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
        configurations.propertiesBuilder(propertiesFile.toFile());
    PropertiesConfiguration config = builder.getConfiguration();

    Class<?> targetClass = targetObject.getClass();
    Field[] fields = targetClass.getDeclaredFields();
    Set<String> allFields = new HashSet<>();
    for (Field field : fields) {
      String fieldName = field.getName();
      allFields.add(fieldName);
      if (config.containsKey(fieldName)) {
        field.setAccessible(true);
        try {
          if (field.getType() == int.class) {
            field.setInt(targetObject, config.getInt(fieldName));
          } else if (field.getType() == long.class) {
            field.setLong(targetObject, config.getLong(fieldName));
          } else if (field.getType() == double.class) {
            field.setDouble(targetObject, config.getDouble(fieldName));
          } else if (field.getType() == String.class) {
            field.set(targetObject, config.getString(fieldName));
          } else {
            throw new RuntimeException("Unsupported configuration data type: " + fieldName);
          }
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Failed to set the value of the field: " + fieldName, e);
        }
      }
    }

    Iterator<String> keys = config.getKeys();
    while (keys.hasNext()) {
      String key = keys.next();
      Preconditions.checkArgument(allFields.contains(key),"Not a valid config property: %s", key);
    }
  }
}

