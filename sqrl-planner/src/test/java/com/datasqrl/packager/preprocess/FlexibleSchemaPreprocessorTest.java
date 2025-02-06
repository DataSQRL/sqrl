package com.datasqrl.packager.preprocess;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.datasqrl.io.schema.flexible.FlexibleSchemaPreprocessor;

class FlexibleSchemaPreprocessorTest {

  @Test
  public void testRegex() {
    var preprocessor = new FlexibleSchemaPreprocessor();
    // Test the regex for the file extension .schema.yml
    assertTrue(preprocessor.getPattern().asMatchPredicate().test("some_table.schema.yml"));
    assertTrue(preprocessor.getPattern().asMatchPredicate().test("table.schema.yml"));
    assertFalse(preprocessor.getPattern().asMatchPredicate().test("schema.yml"));
    assertFalse(preprocessor.getPattern().asMatchPredicate().test("my_schema.yml"));
  }

}