package com.yw.flink.example;

import java.util.Deque;

/**
 * @author yangwei
 */
public class Solution {
    public static void main(String[] args) {

    }

    private static class UnionSet {
        private int[] fa;
        public UnionSet(int n) {
            this.fa = new int[n + 1];
            for (int i = 0; i <= n; i++) {
                fa[i] = i;
            }
        }
        public int get(int x) {
            return fa[x] = (fa[x] == x ? x : get(fa[x]));
        }
        public void merge(int a, int b) {
            fa[get(a)] = get(b);
        }
    }
}
