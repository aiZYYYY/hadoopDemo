package com.itzy.weibo;

import com.itzy.weibo.domain.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: ZY
 * @Date: 2019/5/16 13:27
 * @Version 1.0
 */
public class Weiboutil {

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
     * 发布微博
     * a、微博内容表中数据+1
     * b、向微博收件箱表中加入微博的 Rowkey
     */
    public static void publishContent(Message message) throws IOException {
        Table constent = conn.getTable(TableName.valueOf(Constant.TABLE_CONTENT));
        Table email = conn.getTable(TableName.valueOf(Constant.TABLE_RECEIVE_CONTENT_EMAIL));
        Table relations = conn.getTable(TableName.valueOf(Constant.TABLE_RELATIONS));

        long timeMillis = System.currentTimeMillis();

        String rowKey = message.getUid() + "_" + timeMillis;

        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), timeMillis, Bytes.toBytes(message.getContent()));

        constent.put(put);

        Get get = new Get(Bytes.toBytes(message.getUid()));

        get.addFamily(Bytes.toBytes("fans"));

        Result result = relations.get(get);

        ArrayList<byte[]> fans = new ArrayList<>();

        for (Cell cell : result.rawCells()) {
            fans.add(CellUtil.cloneQualifier(cell));
        }
        if (fans.size() <= 0) {
            return;
        }

        List<Put> puts = new ArrayList<Put>();

        for (byte[] fan : fans) {
            Put fanput = new Put(fan);
            fanput.addColumn(Bytes.toBytes("info"), Bytes.toBytes(message.getUid()), timeMillis, Bytes.toBytes(rowKey));
            puts.add(fanput);
        }
        email.put(puts);
    }

    /**
     * 关注用户逻辑
     * a、在微博用户关系表中，对当前主动操作的用户添加新的关注的好友
     * b、在微博用户关系表中，对被关注的用户添加粉丝（当前操作的用户）
     * c、当前操作用户的微博收件箱添加所关注的用户发布的微博 rowkey
     */
    public static void addAttends(String uid, String... attends) {
        //参数过滤
        if (attends == null || attends.length <= 0 || uid == null || uid.length() <= 0) {
            return;
        }
        try {
            //用户关系表操作对象（连接到用户关系表）
            Table relationsTBL = conn.getTable(TableName.valueOf(Constant.TABLE_RELATIONS));
            List<Put> puts = new ArrayList<Put>();
            //a、在微博用户关系表中，添加新关注的好友
            Put attendPut = new Put(Bytes.toBytes(uid));
            for (String attend : attends) {
                //为当前用户添加关注的人
                attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));
                //b、为被关注的人，添加粉丝
                Put fansPut = new Put(Bytes.toBytes(attend));
                fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
                //将所有关注的人一个一个的添加到 puts（List）集合中
                puts.add(fansPut);
            }
            puts.add(attendPut);
            relationsTBL.put(puts);
            //c.1、微博收件箱添加关注的用户发布的微博内容（content）的 rowkey
            Table contentTBL = conn.getTable(TableName.valueOf(Constant.TABLE_CONTENT));
            Scan scan = new Scan();
            //用于存放取出来的关注的人所发布的微博的 rowkey
            List<byte[]> rowkeys = new ArrayList<byte[]>();
            for (String attend : attends) {
                //过滤扫描 rowkey，即：前置位匹配被关注的人的 uid_
                RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
                //为扫描对象指定过滤规则
                scan.setFilter(filter);
                //通过扫描对象得到 scanner
                ResultScanner result = contentTBL.getScanner(scan);
                //迭代器遍历扫描出来的结果集
                Iterator<Result> iterator = result.iterator();
                while (iterator.hasNext()) {
                    //取出每一个符合扫描结果的那一行数据
                    Result r = iterator.next();
                    for (Cell cell : r.rawCells()) {
                        //将得到的 rowkey 放置于集合容器中
                        rowkeys.add(CellUtil.cloneRow(cell));
                    }
                }
            }
            //c.2、将取出的微博 rowkey 放置于当前操作用户的收件箱中
            if (rowkeys.size() <= 0) {
                return;
            }
            //得到微博收件箱表的操作对象
            Table recTBL = conn.getTable(TableName.valueOf(Constant.TABLE_RECEIVE_CONTENT_EMAIL));
            //用于存放多个关注的用户的发布的多条微博 rowkey 信息
            List<Put> recPuts = new ArrayList<Put>();
            for (byte[] rk : rowkeys) {
                Put put = new Put(Bytes.toBytes(uid));
                //uid_timestamp
                String rowKey = Bytes.toString(rk);
                //借取 uid
                String attendUID = rowKey.substring(0, rowKey.indexOf("_"));
                long timestamp = Long.parseLong(rowKey.substring(rowKey.indexOf("_") + 1));
                //将微博 rowkey 添加到指定单元格中
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendUID), timestamp, rk);
                recPuts.add(put);
            }
            recTBL.put(recPuts);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != conn) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
