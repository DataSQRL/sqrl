package com.datasqrl.physical;

public enum EngineCapability {

    DENORMALIZE,
    TEMPORAL_JOIN,
    TIME_WINDOW_AGGREGATION,
    NOW,
    GLOBAL_SORT,
    MULTI_RANK,
    EXTENDED_FUNCTIONS,
    CUSTOM_FUNCTIONS;

}
