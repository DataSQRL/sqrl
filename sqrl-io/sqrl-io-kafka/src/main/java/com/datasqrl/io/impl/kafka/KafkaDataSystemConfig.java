/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.kafka;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemDiscovery;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.auto.service.AutoService;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.admin.Admin;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Properties;

@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public abstract class KafkaDataSystemConfig {

  public static final String SYSTEM_TYPE = "kafka";

  @NonNull @NotNull @NotEmpty
  List<String> servers;

  String topicPrefix;

  protected boolean rootInitialize(ErrorCollector errors) {
    for (String server : servers) {
      if (Strings.isNullOrEmpty(server)) {
        errors.fatal("Invalid server configuration: %s", server);
      }
    }
    topicPrefix = Strings.isNullOrEmpty(topicPrefix) ? "" : topicPrefix;

    //Check that we can connect to Kafka cluster
    try (Admin admin = Admin.create(getProperties())) {
      String clusterId = admin.describeCluster().clusterId().get();
      if (Strings.isNullOrEmpty(clusterId)) {
        errors.fatal("Could not connect to Kafka cluster - check configuration");
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      errors.fatal("Could not connect to Kafka cluster - check configuration: %s", e);
      return false;
    }
  }

  public String getSystemType() {
    return SYSTEM_TYPE;
  }

  @JsonIgnore
  protected String getServersAsString() {
    return String.join(", ", servers);
  }

  @JsonIgnore
  protected Properties getProperties() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", getServersAsString());
    return properties;
  }



}
