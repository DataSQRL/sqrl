package com.datasqrl.config;

import java.util.Optional;
import java.util.regex.Pattern;

import com.datasqrl.util.StringUtil;
import com.google.common.base.Preconditions;


public class TablePattern {

  private final String pattern;

  private TablePattern(String pattern) {
    Preconditions.checkArgument(isValid(pattern), "Invalid table pattern: %s", pattern);
    this.pattern = pattern;
  }

  public Pattern get(boolean withBoundary) {
    var updatedPattern = pattern;
    if (withBoundary) {
		updatedPattern = addRegexBoundary(pattern);
	}
    return Pattern.compile(updatedPattern);
  }

  public String substitute(String name, Optional<String> prefix, Optional<String> suffix) {
    var nameOffsets = getNameMatchOffsets(pattern);
    var updatedPattern = StringUtil.replaceSubstring(pattern, nameOffsets[0], nameOffsets[1], Pattern.quote(name));
    if (prefix.isPresent()) {
		updatedPattern = Pattern.quote(prefix.get()) + updatedPattern;
	}
    if (suffix.isPresent()) {
		updatedPattern = updatedPattern + Pattern.quote(suffix.get());
	}
    return updatedPattern;
  }

  public static TablePattern of(Optional<String> pattern, String defaultPattern) {
    return new TablePattern(pattern.orElse(defaultPattern));
  }

  public static boolean isValid(String pattern) {
    try {
      Pattern.compile(pattern);
      return getNameMatchOffsets(pattern)!=null;
    } catch (Exception e) {
      return false;
    }
  }

  private static String addRegexBoundary(String pattern) {
    return "^" + pattern + "$";
  }

  public static int[] getNameMatchOffsets(String pattern) {
    var startPos = -1;
    var endPos = -1;
    for (var i = 0; i < pattern.length() - 1; i++) {
      if (pattern.charAt(i) == '(' && pattern.charAt(i + 1) != '?') {
        startPos = i;
        break;
      }
    }

    if (startPos != -1) {
      for (var i = startPos + 1; i < pattern.length(); i++) {
        if (pattern.charAt(i) == ')') {
          endPos = i;
          break;
        }
      }
    }

    if (startPos<0 || endPos<0) {
		return null;
	}
    return new int[]{startPos,endPos};
  }



}
