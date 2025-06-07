/**
 * @author yangwei
 */
public class Solution {
    public static void main(String[] args) {
        int[] nums = {3,1,5, 7, 1,8,9};

        System.out.println(longestSeq(nums));
    }

    private static int longestSeq(int[] nums) {
        int n = nums.length;
        int[] dp = new int[n];
        dp[0] = 1;
        for (int i = 1; i < n; i++) {
            for (int j = i - 1; j >= 0; j--) {
                if (nums[j] < nums[i]) {
                    dp[i] = dp[j] + 1;
                    break;
                }
            }
            dp[i] = Math.max(dp[i], dp[i - 1]);
        }
        return dp[n - 1];
    }
}
