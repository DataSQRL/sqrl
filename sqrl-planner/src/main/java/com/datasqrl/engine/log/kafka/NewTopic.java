package com.datasqrl.engine.log.kafka;

import java.util.List;
import java.util.Map;

import com.datasqrl.engine.database.EngineCreateTable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class NewTopic implements EngineCreateTable {
    private String topicName;
    private String tableName;
    private int numPartitions;
    private short replicationFactor;
    private Map<Integer, List<Integer>> replicasAssignments;
    private Map<String, String> config;

    public NewTopic(String topicName, String tableName) {
        this(topicName, tableName, 1, Short.parseShort("1"), Map.of(), Map.of());
    }
}
