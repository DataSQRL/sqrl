package com.datasqrl.config;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.configuration2.BaseHierarchicalConfiguration;
import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.JSONConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
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

  @NonNull ErrorCollector errors;
  String configFilename;
  @NonNull Configuration config;
  String prefix;

  @Override
  public SqrlConfigCommons getSubConfig(String name) {
    return new SqrlConfigCommons(errors,configFilename,config,getPrefix(name));
  }

  private String getPrefix(String name) {
    return prefix + name + DELIMITER;
  }

  @Override
  public Iterable<String> getLocalKeys() {
    LinkedHashSet<String> subKeys = new LinkedHashSet<>();
    getAllKeys().forEach(suffix -> {
      int index = suffix.indexOf(DELIMITER);
      String simpleKey = index<0?suffix:suffix.substring(0,index);
      if (!Strings.isNullOrEmpty(simpleKey)) subKeys.add(simpleKey);
    });
    return subKeys;
  }

  @Override
  public Iterable<String> getAllKeys() {
    List<String> allKeys = new ArrayList<>();
    config.getKeys(prefix).forEachRemaining(subKey -> {
      String suffix = subKey.substring(prefix.length());
      if (!Strings.isNullOrEmpty(suffix)) allKeys.add(suffix);
    });
    return allKeys;
  }

  @Override
  public boolean containsKey(String key) {
    return config.containsKey(getKey(key));
  }

  @Override
  public <T> Value<T> as(String key, Class<T> clazz) {
    String fullKey = getKey(key);
    if (isBasicClass(clazz)) { //Direct mapping
      if (!config.containsKey(fullKey)) return new ValueImpl<>(fullKey, errors, Optional.empty());
      return new ValueImpl<>(fullKey, errors, Optional.of(config.get(clazz,fullKey)));
    } else {
      //Try to map class by field
      return getSubConfig(key).allAs(clazz);
    }

  }

  private boolean isBasicClass(Class<?> clazz) {
    return clazz.isArray() || clazz.isPrimitive() || String.class.isAssignableFrom(clazz)
        || Number.class.isAssignableFrom(clazz) || Boolean.class.isAssignableFrom(clazz)
        || Duration.class.isAssignableFrom(clazz);
  }

  public <T> Value<T> allAs(Class<T> clazz) {
    errors.checkFatal(!isBasicClass(clazz),"Cannot map configuration onto a basic class: %s", clazz.getName());
    try {
      T value = clazz.newInstance();
      for (Field field : clazz.getDeclaredFields()) {
        field.setAccessible(true);
        Class<?> fieldClass = field.getType();
        Value configValue;
        if (fieldClass.isAssignableFrom(ArrayList.class)) {
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
          configValue = asList(field.getName(),listClass);
        } else {
          configValue = as(field.getName(),fieldClass);
        }
        if (field.getAnnotation(Constraints.Default.class)!=null) {
          configValue.withDefault(field.get(value));
        }
        configValue = Constraints.addConstraints(field, configValue);
        field.set(value, configValue.get());
      }
      return new ValueImpl<>(prefix, errors, Optional.of(value));
    } catch (Exception e) {
      if (e instanceof CollectedException) throw (CollectedException)e;
      throw errors.exception("Could not map configuration values on "
          + "object of clazz [%s]: %s", clazz.getName(), e.getMessage());
    }
  }

  @Override
  public <T> Value<List<T>> asList(String key, Class<T> clazz) {
    String fullKey = getKey(key);
    List<T> list = List.of();
    if (config.containsKey(fullKey)) {
      list = config.getList(clazz,fullKey);
    }
    return new ValueImpl<>(fullKey, errors, Optional.of(list));
  }

  @Override
  public <T> Value<LinkedHashMap<String, T>> asMap(String key, Class<T> clazz) {
    LinkedHashMap<String,T> map = new LinkedHashMap<>();
    SqrlConfig subConfig = getSubConfig(key);
    subConfig.getLocalKeys().forEach(subKey -> map.put(subKey,subConfig.as(subKey,clazz).get()));
    return new ValueImpl<>(getKey(key), errors, Optional.of(map));
  }

  @Override
  public ErrorCollector getErrorCollector() {
    return errors;
  }

  private String getKey(String key) {
    return prefix + key;
  }

  @Override
  public void setProperty(String key, Object value) {
    config.setProperty(getKey(key), value);
  }

  @Override
  public void setProperties(Object value) {
    Class<?> clazz = value.getClass();
    errors.checkFatal(!isBasicClass(clazz),"Cannot set multiple properties from basic class: %s", clazz.getName());
    try {
      for (Field field : clazz.getDeclaredFields()) {
        field.setAccessible(true);
        Object fieldValue = field.get(value);
        if (fieldValue!=null) setProperty(field.getName(),fieldValue);
      }
    } catch (Exception e) {
      if (e instanceof CollectedException) throw (CollectedException)e;
      throw errors.exception("Could not access fields "
          + "of clazz [%s]: %s", clazz.getName(), e.getMessage());
    }
  }

  @Override
  public void copy(SqrlConfig from) {
    Preconditions.checkArgument(from instanceof SqrlConfigCommons);
    SqrlConfigCommons other = (SqrlConfigCommons) from;
    for (String sub : other.getAllKeys()) {
      this.setProperty(sub, other.config.getProperty(other.getKey(sub)));
    }
  }

  @Override
  public void toFile(Path file) {
    Preconditions.checkArgument(Files.isDirectory(file.getParent()));
    Parameters params = new Parameters();
    FileBasedConfigurationBuilder<JSONConfiguration> builder = new FileBasedConfigurationBuilder<>(JSONConfiguration.class)
        .configure(params.fileBased().setFile(file.toFile()));
    try {
      JSONConfiguration outputConfig = builder.getConfiguration();
      outputConfig.append(config.subset(prefix));
      builder.save();
    } catch (ConfigurationException e) {
      throw errors.handle(e);
    }
  }

  @Override
  public SerializedSqrlConfig serialize() {
    Map<String, Object> map = new HashMap<>();
    config.getKeys(prefix).forEachRemaining(key -> {
      map.put(key,config.getProperty(key));
    });
    return new Serialized(configFilename, map, prefix);
  }

  public static SqrlConfig create(ErrorCollector errors) {
    Configuration config = new BaseHierarchicalConfiguration();
    return new SqrlConfigCommons(errors,null,config,"");
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
      String configFilename = firstFile.toString();
      return new SqrlConfigCommons(errors.withConfig(configFilename), configFilename, resultconfig, "");
    } catch (ConfigurationException e) {
      throw errors.handle(e);
    }
  }

  public static SqrlConfig fromURL(ErrorCollector errors, @NonNull URL url) {
    Configurations configs = new Configurations();
    try {
      Configuration config = configs.fileBased(JSONConfiguration.class, url);
      String configFilename = url.toString();
      return new SqrlConfigCommons(errors.withConfig(configFilename), configFilename, config, "");
    } catch (ConfigurationException e) {
      throw errors.handle(e);
    }

  }

  public static class ValueImpl<T> implements Value<T> {

    private final String fullKey;
    private final ErrorCollector errors;
    private Optional<T> property;
    private Optional<T> defaultValue = null;
    private Map<Predicate<T>, String> validators = new HashMap<>(2);

    public ValueImpl(String fullKey, ErrorCollector errors, @NonNull Optional<T> property) {
      this.fullKey = fullKey;
      this.errors = errors;
      this.property = property;
    }

    @Override
    public T get() {
      errors.checkFatal(property.isPresent() || defaultValue!=null, "Could not find key [%s] in configuration", fullKey);
      T value;
      if (property.isPresent()) {
        value = property.get();
        validators.forEach((pred, msg) -> errors.checkFatal(pred.test(value),"Value [%s] for key [%s] is not valid. %s",value, fullKey, msg));
      } else {
        value = defaultValue.orElse(null);
      }
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

  @AllArgsConstructor
  @NoArgsConstructor
  private static class Serialized implements SerializedSqrlConfig {

    String configFilename;
    Map<String,Object> configs;
    String prefix;

    @Override
    public SqrlConfig deserialize(@NonNull ErrorCollector errors) {
      Configuration config = new MapConfiguration(configs);
      return new SqrlConfigCommons(errors.withConfig(configFilename), configFilename, config, prefix);
    }
  }

}
