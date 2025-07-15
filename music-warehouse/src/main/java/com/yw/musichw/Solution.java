package com.yw.musichw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author yangwei
 */
public class Solution {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        lengthOfLongestSubstring("adbasdsancas", list);
        // [d, db, dba, dbas, basd, dsan, dsanc]
        System.out.println(list);
    }
    public static int lengthOfLongestSubstring(String s, List<String> list) {
        if (s == null || s.length() == 0) return 0;
        char[] cs = s.toCharArray();
        int[] arr = new int[256];
        Arrays.fill(arr, -1);
        arr[cs[0]] = 0;
        int n = cs.length, ans = 1, pre = 1;
        for (int i = 1; i < n; i++) {
            pre = Math.min(i - arr[cs[i]], pre + 1);
            if (pre >= ans) {
                System.out.println("pre = " + pre + ", i = " + i);
                // ！！！字符串截取
                list.add(s.substring(Math.max((i - pre), 0) + 1, i + 1));
                ans = pre;
            }
            arr[cs[i]] = i;
        }
        return ans;
    }
}
