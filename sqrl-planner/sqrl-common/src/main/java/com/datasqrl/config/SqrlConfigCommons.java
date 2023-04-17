package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.configuration2.BaseHierarchicalConfiguration;
import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.JSONConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.NodeCombiner;
import org.apache.commons.configuration2.tree.OverrideCombiner;

/**
 * Implementions of {@link SqrlConfig} based on Apache commons configuration (v2)
 */
@Value
public class SqrlConfigCommons implements SqrlConfig {

  private static final char DELIMITER = '.';

  ErrorCollector errors;
  Configuration config;
  String prefix;

  @Override
  public SqrlConfigCommons getSubConfig(String name) {
    return new SqrlConfigCommons(errors,config,getPrefix(name));
  }

  private String getPrefix(String name) {
    return prefix + name + DELIMITER;
  }

  @Override
  public Iterable<String> getKeys() {
    LinkedHashSet<String> subKeys = new LinkedHashSet<>();
    config.getKeys(prefix).forEachRemaining(subKey -> {
      String suffix = subKey.substring(0,prefix.length());
      int index = suffix.indexOf(DELIMITER);
      String simpleKey = index<0?suffix:suffix.substring(index+1);
      subKeys.add(simpleKey);
    });
    return subKeys;
  }

  @Override
  public <T> Value<T> key(String key, Class<T> clazz) {
    String fullKey = getKey(key);
    if (!config.containsKey(fullKey)) return new ValueImpl<>(fullKey, errors, Optional.empty());
    else {
      T value;
      if (clazz.isArray() || clazz.isPrimitive() || String.class.isAssignableFrom(clazz)
          || Number.class.isAssignableFrom(clazz) || Boolean.class.isAssignableFrom(clazz)
      || Duration.class.isAssignableFrom(clazz)) {
        value = config.get(clazz,fullKey);
      } else {
        //Try to map class by field
        SqrlConfig subConfig = getSubConfig(key);
        try {
          T obj = clazz.newInstance();
          for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            Class<?> fieldClass = field.getType();
            Value configValue;
            if (List.class.isAssignableFrom(fieldClass)) {
              Type genericType = field.getGenericType();
              errors.checkFatal(genericType instanceof ParameterizedType,
                  "Field [%s] on class [%s] does not have a valid generic type",
                  field.getName(), clazz.getName());
              ParameterizedType parameterizedType = (ParameterizedType) genericType;
              Type[] typeArguments = parameterizedType.getActualTypeArguments();
              errors.checkFatal(typeArguments.length==1 && typeArguments[0] instanceof Class,
                  "Field [%s] on class [%s] does not have a valid generic type",
                  field.getName(), clazz.getName());
              Class<?> listClass = (Class<?>)typeArguments[0];
              configValue = subConfig.keyList(field.getName(),listClass);
            } else {
              configValue = subConfig.keyList(field.getName(),fieldClass);
            }
            if (field.getAnnotation(Constraints.Default.class)!=null) {
              configValue.withDefault(field.get(obj));
            }
            field.set(obj, configValue.get());
          }
          value = obj;
        } catch (Exception e) {
          throw errors.exception("Could not map configuration values on "
              + "object of clazz [%s]: %s", clazz.getName(), e.getMessage());
        }
      }
      return new ValueImpl<>(fullKey, errors, Optional.of(value));
    }
  }

  @Override
  public <T> Value<List<T>> keyList(String key, Class<T> clazz) {
    String fullKey = getKey(key);
    List<T> list = List.of();
    if (config.containsKey(fullKey)) {
      list = config.getList(clazz,fullKey);
    }
    return new ValueImpl<>(fullKey, errors, Optional.of(list));
  }

  @Override
  public <T> Value<Map<String, T>> keyMap(String key, Class<T> clazz) {
    LinkedHashMap<String,T> map = new LinkedHashMap<>();
    SqrlConfig subConfig = getSubConfig(key);
    subConfig.getKeys().forEach(subKey -> map.put(subKey,subConfig.key(subKey,clazz).get()));
    return new ValueImpl<>(getKey(key), errors, Optional.of(map));
  }

  private String getKey(String key) {
    return prefix + key;
  }



  @Override
  public void setProperty(String key, Object value) {
    config.setProperty(getKey(key), value);
  }

  @Override
  public void toFile(Path file) {
    Preconditions.checkArgument(Files.isDirectory(file.getParent()));
    Parameters params = new Parameters();
    FileBasedConfigurationBuilder<JSONConfiguration> builder = new FileBasedConfigurationBuilder<>(JSONConfiguration.class)
        .configure(params.fileBased().setFile(file.toFile()));
    try {
      JSONConfiguration outputConfig = builder.getConfiguration();
      outputConfig.append(config);
      builder.save();
    } catch (ConfigurationException e) {
      throw errors.handle(e);
    }
  }

  public static SqrlConfig create(ErrorCollector errors) {
    Configuration config = new BaseHierarchicalConfiguration();
    return new SqrlConfigCommons(errors,config,"");
  }

  public static SqrlConfig fromFiles(ErrorCollector errors, @NonNull Path firstFile, Path... otherFiles) {
    Configurations configs = new Configurations();
    Configuration resultconfig;
    try {
      Configuration baseconfig = configs.fileBased(JSONConfiguration.class, firstFile.toFile());

      if (otherFiles.length>0) {
        NodeCombiner combiner = new OverrideCombiner();
        CombinedConfiguration cc = new CombinedConfiguration(combiner);
        for (int i = otherFiles.length-1; i >= 0; i--) { //iterate backwards so last file takes precedence
          Configuration nextconfig = configs.fileBased(JSONConfiguration.class, otherFiles[i].toFile());
          cc.addConfiguration(nextconfig);
        }
        cc.addConfiguration(baseconfig);
        resultconfig = cc;
      } else {
        resultconfig = baseconfig;
      }
      return new SqrlConfigCommons(errors.withConfig(firstFile), resultconfig, "");
    } catch (ConfigurationException e) {
      throw errors.handle(e);
    }
  }

  public static class ValueImpl<T> implements Value<T> {

    private final String fullKey;
    private final ErrorCollector errors;
    private Optional<T> property;
    private Optional<T> defaultValue = Optional.empty();
    private Map<Predicate<T>, String> validators = new HashMap<>(2);

    public ValueImpl(String fullKey, ErrorCollector errors, @NonNull Optional<T> property) {
      this.fullKey = fullKey;
      this.errors = errors;
      this.property = property;
    }

    @Override
    public T get() {
      errors.checkFatal(property.isPresent() || defaultValue.isPresent(), "Could not find key [%s] in configuration", fullKey);
      T value = property.orElseGet(defaultValue::get);
      validators.forEach((pred, msg) -> errors.checkFatal(pred.test(value),"Value [%s] for key [%s] is not valid. %s",value, fullKey, msg));
      return value;
    }

    @Override
    public Value<T> withDefault(T defaultValue) {
      this.defaultValue = Optional.ofNullable(defaultValue);
      return this;
    }

    @Override
    public Value<T> validate(Predicate<T> validator, String msg) {
      validators.put(validator, msg);
      return this;
    }

    @Override
    public Value<T> map(Function<T, T> mapFunction) {
      this.property = this.property.map(mapFunction);
      return this;
    }
  }

}
