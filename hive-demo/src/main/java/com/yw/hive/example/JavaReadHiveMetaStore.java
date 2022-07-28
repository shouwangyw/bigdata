package com.yw.hive.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * @author yangwei
 */
public class JavaReadHiveMetaStore {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hive.metastore.uris", "thrift://node03:9083");
        IMetaStoreClient client = init(conf);

        System.out.println("---------- 1. 获取所有catalogs ----------");
        client.getCatalogs().forEach(System.out::println);

        System.out.println("---------- 2. 获取所有catalogs为hive的描述信息 ----------");
        System.out.println(client.getCatalog("hive").toString());

        System.out.println("---------- 3. 获取所有catalogs为hive的所有database ----------");
        List<String> dbNames = client.getAllDatabases("hive");
        dbNames.forEach(System.out::println);

        System.out.println("---------- 4. 获取catalogs为hive的所有database的描述信息 ----------");
        for (String dbName : dbNames) {
            System.out.println(client.getDatabase("hive", dbName));
        }

        System.out.println("---------- 5. 获取catalogs为hive的所有database的所有表 ----------");
        for (String dbName : dbNames) {
            System.out.println("\t==>> 数据库 " + dbName + " 的所有表: ");
            client.getTables("hive", dbName, "*").forEach(System.out::println);
        }

        System.out.println("---------- 6. 获取catalogs为hive的所有database的所有表的描述信息 ----------");
        for (String dbName : dbNames) {
            List<String> tableNames = client.getTables("hive", dbName, "*");
            for (String tableName : tableNames) {
                System.out.println("\t==>> 数据库 " + dbName + ", 表 " + tableName + " 的描述信息: ");
                Table table = client.getTable("hive", dbName, tableName);
                System.out.println(table.toString());

                System.out.println("\t\t==>> 数据库 " + dbName + ", 表 " + tableName + " 的所有字段信息: ");
                List<FieldSchema> cols = table.getSd().getCols();
                for (FieldSchema col : cols) {
                    System.out.println("\t\t\t" + col.toString());
                    System.out.println("\t\t\tcolName = " + col.getName());
                    System.out.println("\t\t\tcolType = " + col.getType());
                    System.out.println("\t\t\tcolComment = " + col.getComment());
                }
            }
        }

        client.close();
    }

    private static IMetaStoreClient init(Configuration conf) throws MetaException {
        try {
            return RetryingMetaStoreClient.getProxy(conf, false);
        } catch (MetaException e) {
            System.out.println("HMS连接失败: " + e);
            throw e;
        }
    }
}
