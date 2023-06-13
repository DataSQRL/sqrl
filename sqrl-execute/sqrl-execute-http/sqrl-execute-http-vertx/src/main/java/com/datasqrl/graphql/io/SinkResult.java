package com.datasqrl.graphql.io;

import lombok.Value;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Value
public class SinkResult {

    Instant sourceTime;
    Optional<UUID> uuid;

}
