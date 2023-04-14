/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
/**
 * The schema package in SQRL is more like a schema header rather than a full schema. A schema
 * header is like a blueprint for a building. It defines the minimum amount of information needed to
 * construct a plan and start construction. The SQRL schema header defines the minimum necessary to
 * transpile and plan and leaves the type inferencing and other query validation concerns to
 * Calcite.
 * <p>
 * The schema is a hierarchical structure of tables with fields which can support shadowing. Fields
 * can be relationships or columns. Relationships can be either Parent, Child, or Join declarations.
 * Join declarations always preserve the path through the schema and conditions always point to the
 * same columns when it was defined. Columns represent some column name and supports variable
 * shadowing.
 * <p>
 * The schema package also contains a type system for schema inference. However, these are not
 * carried over to the columns as we use Calcite to handle all of the datatypes. Schema elements are
 * connected to calcite by their canonical names, also called its 'id'.
 */
package com.datasqrl.schema;