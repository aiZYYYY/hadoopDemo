package com.atguigu.mr.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author: ZY
 * @Date: 2019/4/25 17:48
 * @Version 1.0
 */
public class TwoFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    //A	I,K,C,B,G,F,H,O,D,
    //友  人人人
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String friend = split[0];
        String[] users = split[1].split(",");
        Arrays.sort(users);
        for (int i = 0; i < users.length - 1; i++) {
            for (int j = i + 1; j < users.length; j++) {

                context.write(new Text(users[i] + "-" + users[j]), new Text(friend));
            }
        }
    }
}
