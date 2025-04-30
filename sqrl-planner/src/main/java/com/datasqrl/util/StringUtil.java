package com.datasqrl.util;

import com.google.common.base.Preconditions;

public class StringUtil {

    public static String removeFromEnd(String base, String remove) {
        Preconditions.checkArgument(base.length()>=remove.length(), "Invalid removal [%s] for [%s]", remove, base);
        Preconditions.checkArgument(base.endsWith(remove), "[%s] does not end in [%s]", base, remove);

        return base.substring(0,base.length()-remove.length());
    }

    public static String replaceSubstring(String original, int start, int end, String replacement) {
        if (start < 0 || end > original.length() || start > end) {
            throw new IllegalArgumentException("Invalid start or end position");
        }
        var before = original.substring(0, start);
        var after = original.substring(end);
        return before + replacement + after;
    }

}
