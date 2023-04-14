/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FileFormat;
import com.datasqrl.io.formats.FormatConfiguration;
import com.datasqrl.io.impl.file.FileUtil;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.util.StringUtil;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.Admin;


public abstract class KafkaDataSystem {

  @AllArgsConstructor
  @Getter
  public static class Connector implements DataSystemConnector, Serializable {

    final Properties properties;
    final String topicPrefix;

    @Override
    public boolean hasSourceTimestamp() {
      return true;
    }

    @Override
    public String getSystemType() {
      return KafkaDataSystemConfig.SYSTEM_TYPE;
    }

    public Properties getProperties(String groupId) {
      Properties copy = new Properties();
      copy.putAll(properties);
      if (!Strings.isNullOrEmpty(groupId)) {
        copy.put("group.id", groupId);
      }
      return copy;
    }

    public String getServersAsString() {
      return properties.getProperty("bootstrap.servers");
    }

  }

  public static class Discovery extends Connector implements DataSystemDiscovery, Serializable {

    public static String[] TOPIC_SUFFIX = {".", "/", "_"};

    final KafkaDataSystemConfig.Connector connectorConfig;

    public Discovery(Properties properties, String topicPrefix,
        KafkaDataSystemConfig.Connector connectorConfig) {
      super(properties, topicPrefix);
      this.connectorConfig = connectorConfig;
    }

    @Override
    public @NonNull Optional<String> getDefaultName() {
      if (!Strings.isNullOrEmpty(topicPrefix)) {
        //See if we need to truncate suffix
        String name = topicPrefix;
        for (String suffix : TOPIC_SUFFIX) {
          if (name.endsWith(suffix)) {
            name = StringUtil.removeFromEnd(name, suffix);
            break;
          }
        }
        name = name.trim();
        if (name.length() > 2) {
          return Optional.of(name);
        }
      }
      return Optional.empty();
    }

    @Override
    public boolean requiresFormat(ExternalDataType type) {
      if (type.isSource()) {
        return false;
      } else {
        return true;
      }
    }


    @Override
    public Collection<TableConfig> discoverSources(
        @NonNull DataSystemConfig config, @NonNull ErrorCollector errors) {
      List<TableConfig> tables = new ArrayList<>();
      Set<String> topicNames = Collections.EMPTY_SET;
      try (Admin admin = Admin.create(getProperties(null))) {
        topicNames = admin.listTopics().names().get();
      } catch (Exception e) {
        errors.warn("Could not discover Kafka topics: %s", e);
      }
      FormatConfiguration format = config.getFormat();
      NameCanonicalizer canonicalizer = config.getNameCanonicalizer();
      topicNames.stream().filter(n -> n.startsWith(topicPrefix))
          .map(n -> n.substring(topicPrefix.length()).trim())
          .filter(Predicate.not(Strings::isNullOrEmpty))
          .forEach(name -> {
            TableConfig.TableConfigBuilder tblBuilder = TableConfig.copy(config);
            tblBuilder.connector(connectorConfig);

            if (format != null) {
              if (Name.validName(name)) {
                tblBuilder.identifier(name).name(name);
                tblBuilder.format(format);
              } else {
                errors.warn("Topic [%s] has an invalid name and is not added as a table", name);
                return;
              }
            } else {
              //try to infer format from topic name
              Pair<String, String> components = FileUtil.separateExtension(name);
              FileFormat ff = FileFormat.getFormat(components.getValue());
              if (ff != null && Name.validName(components.getKey())) {
                tblBuilder.identifier(name).name(components.getKey());
                tblBuilder.format(ff.getImplementation().getDefaultConfiguration());
              } else {
                errors.warn("Could not infer format for topic [%s] and is not added as a table",
                    name);
                return;
              }
            }
            tables.add(tblBuilder.build());
          });
      return tables;
    }
  }
}
