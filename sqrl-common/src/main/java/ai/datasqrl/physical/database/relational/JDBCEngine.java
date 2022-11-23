package ai.datasqrl.physical.database.relational;

import ai.datasqrl.config.provider.Dialect;
import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.ExecutionEngine;

import java.util.EnumMap;
import java.util.EnumSet;

import static ai.datasqrl.physical.EngineCapability.*;

public class JDBCEngine extends ExecutionEngine.Impl {

    public static final EnumMap<Dialect,EnumSet<EngineCapability>> CAPABILITIES_BY_DIALECT = new EnumMap<Dialect, EnumSet<EngineCapability>>(
        Dialect.class);
    static {
        CAPABILITIES_BY_DIALECT.put(Dialect.POSTGRES,
                EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
    }

    public JDBCEngine(Dialect dialect) {
        super(Type.DATABASE, CAPABILITIES_BY_DIALECT.get(dialect));
    }
}
