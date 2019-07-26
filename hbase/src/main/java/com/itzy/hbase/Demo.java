package com.itzy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @Author: ZY
 * @Date: 2019/5/9 14:43
 * @Version 1.0
 */
public class Demo {

    static Configuration conf;
    static Connection conn;
    private static Admin admin = null;

    /**
     * HBase文件配置
     */
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
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(TableName tableName) throws IOException {

        return admin.tableExists(tableName);
    }

    private static byte[][] getSplitKeys() {
        String[] keys = new String[] { "10|", "20|", "30|", "40|", "50|", "60|", "70|", "80|", "90|" };
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }
    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void creatTable(TableName tableName, String... columnFamily) throws IOException {
        byte[][] splitKeys =getSplitKeys();
        if (isTableExist(tableName)) {
            System.out.println("表:" + tableName + "已经存在");
        } else {
            //创建表属性对象
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor,splitKeys);
            System.out.println("表:" + tableName + "创建成功！");
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(TableName tableName) throws IOException {
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }

    /**
     * 向表中添加数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(TableName tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        Table table = conn.getTable(tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向 Put 对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("插入数据成功");
    }

    /**
     * 删除所有数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteMultiRow(TableName tableName, String... rows) throws IOException {

        Table table = conn.getTable(tableName);
        ArrayList<Delete> deletes = new ArrayList<>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deletes.add(delete);
        }
        table.delete(deletes);
        table.close();
    }

    /**
     * 获取所有数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(TableName tableName) throws IOException {
        Table table = conn.getTable(tableName);
        //得到用于扫描 region 的对象
        Scan scan = new Scan();
        //使用 Table 得到 resultcanner 实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                sout(cell);
            }
        }
    }

    private static void sout(Cell cell) {
        //得到 rowkey
        System.out.println(" 行 键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
        //得到列族
        System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
        System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
        System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
        System.out.println("----------------------华丽的分割线--------------------------");
    }

    /**
     * 获取某一行数据
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(TableName tableName, String rowKey) throws IOException{
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //显示所有版本
        get.setMaxVersions(3);
        //显示指定时间戳的版本
        //get.setTimeStamp(System.currentTimeMillis());
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            sout(cell);
        }
    }

    /**
     * 获取某一行指定“列族: 列”的数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(TableName tableName, String rowKey, String family, String qualifier) throws IOException{
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            sout(cell);
        }
    }


    public static void main(String[] args) throws IOException {
//        System.out.println(isTableExist(TableName.valueOf("sixi:test")));
//        System.out.println(isTableExist(TableName.valueOf("bigdata:student")));

//        creatTable(TableName.valueOf("zhangyu:student"),"info","msg");

//        addRowData(TableName.valueOf("bigdata:student"), "1004", "info", "age", "12");
//        createNameSpace("zhangyu");
//        getAllRows(TableName.valueOf("bigdata:student"));
        long l = System.currentTimeMillis();
        getRow(TableName.valueOf("sixi:aliorder"),"2935806");
        long l1 = System.currentTimeMillis();
        System.out.println(l1 - l);
        //      getRowQualifier(TableName.valueOf("bigdata:student"),"1001","info","age");
//        creatTable(TableName.valueOf("sixi:fei"),"info");
    }
}
