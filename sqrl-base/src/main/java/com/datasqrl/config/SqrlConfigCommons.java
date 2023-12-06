package com.datasqrl.config;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
  private static final String DELIMITER_STR = DELIMITER+"";
  private static final String DOUBLE_DELIMITER = DELIMITER_STR + DELIMITER_STR;

  @NonNull ErrorCollector errors;
  String configFilename;
  @NonNull Configuration config;
  String prefix;

  @Override
  public SqrlConfigCommons getSubConfig(String name) {
    return new SqrlConfigCommons(errors,configFilename,config,getPrefix(name));
  }

  private String getPrefix(String name) {
    return getFullKey(name) + DELIMITER;
  }

  private String getFullKey(String key) {
    key = key.replace(DELIMITER_STR,DOUBLE_DELIMITER);
    return expandKey(key);
  }

  private String expandKey(String relativeKey) {
    return prefix + relativeKey;
  }

  private String relativeKey(String fullKey) {
    return fullKey.substring(prefix.length());
  }

  private static String getLocalKey(String key) {
    //find the first delimiter that isn't a double delimiter
    int index=0;
    while ((index = key.indexOf(DELIMITER,index))>=0) {
      if (key.length()>index+2 && key.charAt(index+1)==DELIMITER) {
        index = index+2;
      } else {
        key = key.substring(0,index);
        break;
      }
    }
    return key.replace(DOUBLE_DELIMITER,DELIMITER_STR);
  }

  @Override
  public Iterable<String> getKeys() {
    LinkedHashSet<String> localKeys = new LinkedHashSet<>();
    getAllKeys().forEach(relativeKey -> {
      String localKey = getLocalKey(relativeKey);
      if (!Strings.isNullOrEmpty(localKey)) localKeys.add(localKey);
    });
    return localKeys;
  }

  private Iterable<String> getAllKeys() {
    List<String> allKeys = new ArrayList<>();
    config.getKeys(prefix).forEachRemaining(subKey -> {
      String suffix = relativeKey(subKey);
      if (!Strings.isNullOrEmpty(suffix)) allKeys.add(suffix);
    });
    return allKeys;
  }

  @Override
  public boolean containsKey(String key) {
    return config.containsKey(getFullKey(key));
  }

  @Override
  public <T> Value<T> as(String key, Class<T> clazz) {
    String fullKey = getFullKey(key);
    String expandKey = expandKey(key);
    if (isBasicClass(clazz)) { //Direct mapping
      if (!config.containsKey(fullKey)) return new ValueImpl<>(expandKey, errors, Optional.empty());
      return new ValueImpl<>(expandKey, errors, Optional.of(config.get(clazz,fullKey)));
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
        if (Modifier.isStatic(field.getModifiers())) continue;
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
          + "object of clazz [%s]: %s", clazz.getName(), e.toString());
    }
  }

  @Override
  public <T> Value<List<T>> asList(String key, Class<T> clazz) {
    Preconditions.checkArgument(isBasicClass(clazz),"Not a basic class: " + clazz);
    String fullKey = getFullKey(key);
    List<T> list = List.of();
    if (config.containsKey(fullKey)) {
      list = config.getList(clazz,fullKey);
    }
    return new ValueImpl<>(expandKey(key), errors, Optional.of(list));
  }

  @Override
  public <T> Value<LinkedHashMap<String, T>> asMap(String key, Class<T> clazz) {
    LinkedHashMap<String,T> map = new LinkedHashMap<>();
    SqrlConfig subConfig = getSubConfig(key);
    subConfig.getKeys().forEach(subKey -> map.put(subKey,subConfig.as(subKey,clazz).get()));
    return new ValueImpl<>(expandKey(key), errors, Optional.of(map));
  }

  @Override
  public ErrorCollector getErrorCollector() {
    return errors;
  }

  @Override
  public void setProperty(String key, Object value) {
    config.setProperty(getFullKey(key), value);
  }

  @Override
  public void setProperties(Object value) {
    Class<?> clazz = value.getClass();
    errors.checkFatal(!isBasicClass(clazz),"Cannot set multiple properties from basic class: %s", clazz.getName());
    try {
      for (Field field : clazz.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers())) continue;
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
      config.setProperty(expandKey(sub), other.config.getProperty(other.expandKey(sub)));
    }
  }

  @Override
  public void toFile(Path file, boolean pretty) {
    try {
      if (!Files.exists(file) || Files.readString(file).isBlank()) {
        Files.writeString(file, "{}", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      }
      Parameters params = new Parameters();
      FileBasedConfigurationBuilder<JSONConfiguration> builder = new FileBasedConfigurationBuilder<>(JSONConfiguration.class)
              .configure(params.fileBased().setFile(file.toFile()));
      JSONConfiguration outputConfig = builder.getConfiguration();
      outputConfig.append(config.subset(prefix));
      builder.save();

      //Pretty print
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(file.toFile());
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(),jsonNode);
    } catch (ConfigurationException | IOException e) {
      throw errors.withConfig(file).handle(e);
    }
  }

  @Override
  public Map<String, Object> toMap() {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    getKeys().forEach(localKey -> {
      String fullKey = getFullKey(localKey);
      if (config.containsKey(fullKey)) {
        Object value = config.getProperty(fullKey);
        //TODO: this does not interpolate secrets. need to check type and then use type specific access method
        map.put(localKey, value);
      } else {
        SqrlConfigCommons subConfig = getSubConfig(localKey);
        map.put(localKey, subConfig.toMap());
      }
    });
    return map;
  }

  @Override
  public Map<String, String> toStringMap() {
    LinkedHashMap<String, String> map = new LinkedHashMap<>();
    toMap().forEach((k,v) -> map.put(k,String.valueOf(v)));
    return map;
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
    ErrorCollector localErrors = errors.withConfig(firstFile);
    try {
      Configuration baseconfig = configs.fileBased(JSONConfiguration.class, firstFile.toFile());

      if (otherFiles.length>0) {
        NodeCombiner combiner = new OverrideCombiner();
        CombinedConfiguration cc = new CombinedConfiguration(combiner);
        for (int i = otherFiles.length-1; i >= 0; i--) { //iterate backwards so last file takes precedence
          localErrors = errors.withConfig(otherFiles[i]);
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
      throw localErrors.handle(e);
    }
  }

  public static SqrlConfig fromURL(ErrorCollector errors, @NonNull URL url) {
    Configurations configs = new Configurations();
    try {
      Configuration config = configs.fileBased(JSONConfiguration.class, url);
      String configFilename = url.toString();
      return new SqrlConfigCommons(errors.withConfig(configFilename), configFilename, config, "");
    } catch (ConfigurationException e) {
      throw errors.withConfig(url.toString()).handle(e);
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
  @Getter
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  public static class Serialized implements SerializedSqrlConfig {

    String configFilename;
    Map<String,Object> configs;
    String prefix;

    @Override
    public SqrlConfig deserialize(@NonNull ErrorCollector errors) {
      Configuration config = new BaseHierarchicalConfiguration();
      configs.forEach(config::setProperty);
      ErrorCollector configErrors = errors;
      if (!Strings.isNullOrEmpty(configFilename)) configErrors = configErrors.withConfig(configFilename);
      return new SqrlConfigCommons(configErrors, configFilename, config, prefix);
    }
  }

}
