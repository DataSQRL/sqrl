package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlexibleSchemaPreprocessorTest {

  @Test
  public void testRegex() {
    FlexibleSchemaPreprocessor preprocessor = new FlexibleSchemaPreprocessor(ErrorCollector.root());
    // Test the regex for the file extension .schema.yml
    assertTrue(preprocessor.getPattern().asMatchPredicate().test("some_table.schema.yml"));
    assertTrue(preprocessor.getPattern().asMatchPredicate().test("table.schema.yml"));
    assertFalse(preprocessor.getPattern().asMatchPredicate().test("schema.yml"));
    assertFalse(preprocessor.getPattern().asMatchPredicate().test("my_schema.yml"));
  }

}