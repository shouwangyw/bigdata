package com.yw.hbase.p04;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author yangwei
 */
public class PhoenixSearch {
    private static final String URL = "jdbc:phoenix:node03:2181";
    private static final String SQL = "select * from USER_PHOENIX";
    public static void main(String[] args) throws Exception {
        try (Connection connection = DriverManager.getConnection(URL);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(SQL)) {
            System.out.printf("%-15s%-15s%-15s\n", "CITY", "POPULATION", "STATE");
            while (rs.next()) {
                System.out.printf("%-15s%-15s%-15s\n", rs.getString("city"),
                        rs.getString("POPULATION"), rs.getString("STATE"));
            }
            System.out.println("---------------------------------------------");
        }
    }
}
