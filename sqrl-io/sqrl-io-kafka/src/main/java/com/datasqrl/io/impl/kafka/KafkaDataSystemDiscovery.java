package com.datasqrl.io.impl.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FileFormatExtension;
import com.datasqrl.io.impl.file.FileUtil;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.util.StringUtil;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaDataSystemDiscovery extends DataSystemDiscovery.Base {

  private final String topicPrefix;
  private final Properties kafkaProperties;

  public KafkaDataSystemDiscovery(TableConfig genericTable, String topicPrefix,
      Properties kafkaProperties) {
    super(genericTable);
    this.topicPrefix = topicPrefix;
    this.kafkaProperties = kafkaProperties;
  }

  public Properties getProperties(String groupId) {
    Properties copy = new Properties();
    copy.putAll(kafkaProperties);
    if (!Strings.isNullOrEmpty(groupId)) {
      copy.put("group.id", groupId);
    }
    return copy;
  }


  @Override
  public boolean requiresFormat(ExternalDataType type) {
    return true;
  }


  @Override
  public Collection<TableConfig> discoverSources(@NonNull ErrorCollector errors) {
    List<TableConfig> tables = new ArrayList<>();
    Set<String> topicNames = Collections.EMPTY_SET;
    try (Admin admin = Admin.create(getProperties(null))) {
      topicNames = admin.listTopics().names().get();
    } catch (Exception e) {
      errors.warn("Could not discover Kafka topics: %s", e);
    }
    topicNames.stream()
        .map(String::trim)
        .filter(n -> n.startsWith(topicPrefix) && n.length()>topicPrefix.length())
        .forEach(topicName -> {
          String nameStr = topicName.substring(topicPrefix.length());
          if (!Name.validName(nameStr)) {
            errors.warn("Topic [%s] has an invalid name [%s] and is not added as a table", topicName, nameStr);
            return;
          }
          Name name = Name.system(nameStr);
          TableConfig.Builder tblBuilder = copyGeneric(name, topicName, ExternalDataType.source);
          tables.add(tblBuilder.build());
        });
    return tables;
  }
}
