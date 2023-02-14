package com.datasqrl.packager.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class Serializer {

    public static ObjectMapper mapper = getMapper();

    public static<O> O read(Path file, String field, Class<O> clazz) throws IOException {
        JsonNode jsonNode = mapper.readValue(file.toFile(), JsonNode.class);
        return mapper.treeToValue(jsonNode.get(field),clazz);
    }

    public static boolean hasField(Path file, String field) throws IOException {
        JsonNode jsonNode = mapper.readValue(file.toFile(), JsonNode.class);
        return jsonNode.has(field) && jsonNode.hasNonNull(field);
    }

    public static<O> void write(Path file, O object) throws IOException {
        mapper.writeValue(file.toFile(),object);
    }

    public static JsonNode combineFiles(List<Path> packageFiles) throws IOException {
        Preconditions.checkArgument(!packageFiles.isEmpty());
        JsonNode combined = mapper.readValue(packageFiles.get(0).toFile(), JsonNode.class);
        for (int i = 1; i < packageFiles.size(); i++) {
            combined = mapper.readerForUpdating(combined)
                    .readValue(packageFiles.get(i).toFile());
        }
        return combined;
    }

    private static ObjectMapper getMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    }

}