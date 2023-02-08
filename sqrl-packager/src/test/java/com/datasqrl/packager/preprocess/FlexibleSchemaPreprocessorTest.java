package com.datasqrl.packager.preprocess;

import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.error.ErrorCollector;
import org.junit.jupiter.api.Test;

class FlexibleSchemaPreprocessorTest {

  @Test
  public void testRegex() {
    FlexibleSchemaPreprocessor preprocessor = new FlexibleSchemaPreprocessor(ErrorCollector.root());
    // Test the regex for the file extension .schema.yml
    assertTrue(preprocessor.getPattern().asMatchPredicate().test("schema.yml"));
    assertTrue(preprocessor.getPattern().asMatchPredicate().test("table.schema.yml"));
    assertFalse(preprocessor.getPattern().asMatchPredicate().test("my_schema.yml"));
  }

}