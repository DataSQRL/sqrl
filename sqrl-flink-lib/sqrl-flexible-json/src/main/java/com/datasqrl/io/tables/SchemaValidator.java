/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.SourceRecord;

import java.io.Serializable;

public interface SchemaValidator extends Serializable {

    SourceRecord.Named verifyAndAdjust(SourceRecord.Raw record, ErrorCollector errors);

}
