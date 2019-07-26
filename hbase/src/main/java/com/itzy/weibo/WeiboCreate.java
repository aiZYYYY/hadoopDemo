package com.itzy.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: ZY
 * @Date: 2019/5/16 13:56
 * @Version 1.0
 */
public class WeiboCreate {

    static Configuration conf;
    static Connection conn;
    static Admin admin;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hbase01,hbase02,hbase03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     *
     * @param conn
     * @param admin
     */
    private static void close(Connection conn, Admin admin) {
        if (null != conn) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != admin) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建namespace
     *
     * @param nameSpace
     * @throws IOException
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        try {
            admin.getNamespaceDescriptor(nameSpace);
        } catch (NamespaceNotFoundException e) {
            //若发生特定的异常，即找不到命名空间，则创建命名空间
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
            admin.createNamespace(namespaceDescriptor);
            System.out.println("namespace:" + nameSpace + "创建成功");
        } finally {
            close(conn, admin);
        }
    }

    /**
     * 创建微博内容表
     * Table Name:weibo:content
     * RowKey:用户 ID_时间戳
     * ColumnFamily:info
     * ColumnLabel:标题 内容  图片 URL
     * Version:1 个版本
     */
    public static void createTableContent() {

        try {
            //创建表描述
            HTableDescriptor content = new HTableDescriptor(TableName.valueOf(Constant.TABLE_CONTENT));

            //创建列族描述
            HColumnDescriptor info = new HColumnDescriptor("info");

            //设置块缓存
            info.setBlockCacheEnabled(true);

            //设置块缓存大小
            info.setBlocksize(2097152);

            //设置压缩方式
            // info.setCompressionType(Algorithm.SNAPPY);

            //设置版本确界
            info.setMaxVersions(1);
            info.setMinVersions(1);

            content.addFamily(info);

            admin.createTable(content);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(conn, admin);
        }
    }

    /**
     * 用户关系表
     * Table Name:weibo:relations
     * RowKey:用户 ID
     * ColumnFamily:attends,fans
     * ColumnLabel:关注用户 ID，粉丝用户 ID
     * ColumnValue:用户 ID
     * Version：1 个版本
     */
    public static void createTableRelations() {
        try {
            HTableDescriptor relations = new HTableDescriptor(TableName.valueOf(Constant.TABLE_RELATIONS));
            //关注的人的列族
            HColumnDescriptor attends = new HColumnDescriptor(Bytes.toBytes("attends"));
            //设置块缓存
            attends.setBlockCacheEnabled(true);
            //设置块缓存大小
            attends.setBlocksize(2097152);
            //设置压缩方式
            // info.setCompressionType(Algorithm.SNAPPY);
            //设置版本确界
            attends.setMaxVersions(1);
            attends.setMinVersions(1);
            //粉丝列族
            HColumnDescriptor fans = new HColumnDescriptor(Bytes.toBytes("fans"));
            fans.setBlockCacheEnabled(true);
            fans.setBlocksize(2097152);
            fans.setMaxVersions(1);
            fans.setMinVersions(1);
            relations.addFamily(attends);
            relations.addFamily(fans);
            admin.createTable(relations);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(conn, admin);
        }
    }

    /**
     * 创建微博收件箱表
     * Table Name: weibo:receive_content_email
     * RowKey:用户 ID
     * ColumnFamily:info
     * ColumnLabel:用户 ID-发布微博的人的用户 ID
     * ColumnValue:关注的人的微博的 RowKey
     * Version:1000
     */
    public static void createTableReceiveContentEmail() {
        try {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Constant.TABLE_RECEIVE_CONTENT_EMAIL));

            HColumnDescriptor info = new HColumnDescriptor("info");

            info.setBlockCacheEnabled(true);

            info.setBlocksize(2097152);

            info.setMaxVersions(1000);

            info.setMinVersions(1000);
            tableDescriptor.addFamily(info);
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(conn, admin);
        }

    }
}
