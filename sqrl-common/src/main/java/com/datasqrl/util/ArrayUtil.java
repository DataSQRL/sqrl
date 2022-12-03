package com.datasqrl.util;

public class ArrayUtil {

    public static boolean contains(int[] arr, int value, int endIndex) {
        for (int i = 0; i < endIndex; i++) {
            if (arr[i]==value) return true;
        }
        return false;
    }

}
