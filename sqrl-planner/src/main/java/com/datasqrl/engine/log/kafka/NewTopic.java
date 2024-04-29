package com.datasqrl.engine.log.kafka;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class NewTopic {
    private String name;
    private int numPartitions;
    private short replicationFactor;
    private Map<Integer, List<Integer>> replicasAssignments;
    private Map<String, String> config;
}
