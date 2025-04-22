package com.datasqrl.graphql;

import java.util.Optional;

import com.google.common.base.Strings;

public enum APIType {

    GraphQL;

    public static Optional<APIType> get(String apiType) {
        if (Strings.isNullOrEmpty(apiType)) {
            return Optional.empty();
        }
        apiType = apiType.trim();
        for (APIType a : values()) {
            if (a.name().equalsIgnoreCase(apiType)) {
                return Optional.of(a);
            }
        }
        return Optional.empty();
    }

}
