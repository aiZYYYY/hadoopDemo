package com.itzy.weibo;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Author: ZY
 * @Date: 2019/5/16 13:29
 * @Version 1.0
 */
public class Constant {

    //名称空间\
    public static final String NAMESPACE ="weibo";

    //微博内容表
    public static final byte[] TABLE_CONTENT = Bytes.toBytes("weibo:content");

    //用户关系表的表名
    public static final byte[] TABLE_RELATIONS = Bytes.toBytes("weibo:relations");

    //微博收件箱表的表名
    public static final byte[] TABLE_RECEIVE_CONTENT_EMAIL = Bytes.toBytes("weibo:receive_content_email");

}
