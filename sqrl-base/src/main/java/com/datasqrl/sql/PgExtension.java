package com.datasqrl.sql;

import com.datasqrl.function.SqrlFunction;

import java.util.Set;

// Service loader interface
public interface PgExtension {
    Class typeClass();

    Set<SqrlFunction> operators();

    SqlDDLStatement getExtensionDdl();
}
