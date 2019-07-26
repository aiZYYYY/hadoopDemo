package com.atguigu.mr.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: ZY
 * @Date: 2019/4/25 16:54
 * @Version 1.0
 */
public class OneFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取每一行
        String line = value.toString();
        String[] split = line.split(":");

        //获取到用户和其好友
        String user = split[0];
        String[] friends = split[1].split(",");

        //循环写出
        //B,A  C,A  D,A
        for (String friend : friends) {
            context.write(new Text(friend),new Text(user));
        }
    }
}

