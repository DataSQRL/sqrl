package com.datasqrl.text;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class SplitTest {

  private final Split splitFunction = new Split();

  // Test for "Returns an array of substrings by splitting the input string based on the given delimiter."
  @Test
  public void testSplitWithDelimiter() {
    String[] result = splitFunction.eval("apple,banana,cherry", ",");
    assertArrayEquals(new String[]{"apple", "banana", "cherry"}, result);
  }

  // Test for "If the delimiter is not found in the string, the original string is returned as the only element in the array."
  @Test
  public void testSplitWithNoDelimiterInString() {
    String[] result = splitFunction.eval("apple", ",");
    assertArrayEquals(new String[]{"apple"}, result);
  }

  // Test for "If the delimiter is empty, every character in the string is split."
  @Test
  public void testSplitWithEmptyDelimiter() {
    String[] result = splitFunction.eval("apple", "");
    assertArrayEquals(new String[]{"a", "p", "p", "l", "e"}, result);
  }

  // Test for "If the string is null, a null value is returned."
  @Test
  public void testSplitWithNullText() {
    String[] result = splitFunction.eval(null, ",");
    assertNull(result);
  }

  // Test for "If the delimiter is null, a null value is returned."
  @Test
  public void testSplitWithNullDelimiter() {
    String[] result = splitFunction.eval("apple,banana,cherry", null);
    assertNull(result);
  }

  // Test for "If the delimiter is found at the beginning of the string, an empty string is added to the array."
  @Test
  public void testSplitWithDelimiterAtBeginning() {
    String[] result = splitFunction.eval(",apple,banana,cherry", ",");
    assertArrayEquals(new String[]{"", "apple", "banana", "cherry"}, result);
  }

  // Test for "If the delimiter is found at the end of the string, an empty string is added to the array."
  @Test
  public void testSplitWithDelimiterAtEnd() {
    String[] result = splitFunction.eval("apple,banana,cherry,", ",");
    assertArrayEquals(new String[]{"apple", "banana", "cherry", ""}, result);
  }

  // Test for "If there are contiguous delimiters, then an empty string is added to the array."
  @Test
  public void testSplitWithContiguousDelimiters() {
    String[] result = splitFunction.eval("apple,,banana,cherry", ",");
    assertArrayEquals(new String[]{"apple", "", "banana", "cherry"}, result);
  }
}
