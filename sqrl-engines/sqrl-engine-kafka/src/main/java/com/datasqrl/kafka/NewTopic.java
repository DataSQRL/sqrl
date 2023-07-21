package com.datasqrl.kafka;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;


@AllArgsConstructor
@NoArgsConstructor
@Getter
public class NewTopic {
    private String name;
    private int numPartitions;
    private short replicationFactor;
    private Map<Integer, List<Integer>> replicasAssignments;
    private Map<String, String> config;

    public <T> NewTopic(String topicName) {
      name = topicName;
    }
}
