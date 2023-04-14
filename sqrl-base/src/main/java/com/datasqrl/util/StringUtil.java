package com.datasqrl.util;

import com.google.common.base.Preconditions;

public class StringUtil {

    public static String removeFromEnd(String base, String remove) {
        Preconditions.checkArgument(base.length()>=remove.length(), "Invalid removal [%s] for [%s]", remove, base);
        Preconditions.checkArgument(base.endsWith(remove), "[%s] does not end in [%s]", base, remove);

        return base.substring(0,base.length()-remove.length());
    }

}
