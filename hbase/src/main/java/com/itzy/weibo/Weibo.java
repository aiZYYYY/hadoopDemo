package com.itzy.weibo;

import com.itzy.weibo.domain.Message;

import java.io.IOException;

/**
 * @Author: ZY
 * @Date: 2019/5/16 13:28
 * @Version 1.0
 */
public class Weibo {
    public static void main(String[] args) throws IOException {
        //WeiboCreate.createNameSpace(Constant.NAMESPACE);
        //WeiboCreate.createTableContent();
        //WeiboCreate.createTableReceiveContentEmail();
        //WeiboCreate.createTableRelations();
        Message message = new Message("0002","1","张宇你好啊,111111");
        Weiboutil.publishContent(message);
       // Weiboutil.addAttends("0001","0002,0003,0004,0005");

//        Weiboutil.addAttends("0001","0002","0006","0003","0004","0005");
    }

}
