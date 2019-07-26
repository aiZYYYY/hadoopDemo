package com.atguigu.mr.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: ZY
 * @Date: 2019/4/25 17:11
 * @Version 1.0
 */
public class OneFriendsReduce extends Reducer<Text, Text, Text, Text> {

    /**
     *
     *    BA CA DA FA EA OA
     *    AB CB EB KB
     *    FC AC DC IC
     *    AD ED FD LD
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();

        for (Text user : values) {
            sb.append(user).append(",");
        }

        context.write(key,new Text(sb.toString()));



    }
}
