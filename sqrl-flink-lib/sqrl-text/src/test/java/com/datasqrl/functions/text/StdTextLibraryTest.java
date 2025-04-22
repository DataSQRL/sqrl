package com.datasqrl.functions.text;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.datasqrl.text.TextFunctions;

public class StdTextLibraryTest {

  @Test
  public void testFormat() {
    var format = "Hello, %s";
    assertEquals("Hello, World", TextFunctions.FORMAT.eval(format, "World"));
    format = "Count: %s, %s, %s, %s";
    assertEquals("Count: 1, 2, 3, 4", TextFunctions.FORMAT.eval(format, "1", "2", "3", "4"));
  }

  @Test
  public void testSearch() {
    assertEquals(1.0/2, TextFunctions.TEXT_SEARCH.eval("Hello World", "hello john"));
    assertEquals(1.0/2, TextFunctions.TEXT_SEARCH.eval("Hello World", "what a world we live in, john"));
    assertEquals(1.0, TextFunctions.TEXT_SEARCH.eval("Hello World", "what a world we live in, john! Hello john"));
    assertEquals(2.0/3, TextFunctions.TEXT_SEARCH.eval("one two THREE", "we are counting", "one two four five six"));
    assertEquals(1.0, TextFunctions.TEXT_SEARCH.eval("one two THREE", "we are counting", "one two four five six", "three forty fiv"));
    assertEquals(0, TextFunctions.TEXT_SEARCH.eval("one two THREE", "what a world we live in, john!"," Hello john"));
  }


}
