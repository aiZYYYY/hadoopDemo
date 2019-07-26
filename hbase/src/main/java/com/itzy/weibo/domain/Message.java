package com.itzy.weibo.domain;

/**
 * @Author: ZY
 * @Date: 2019/5/16 14:37
 * @Version 1.0
 */
public class Message {

    private String uid;
    private String timestamp;
    private String content;

    public Message(String uid, String timestamp, String content) {
        this.uid = uid;
        this.timestamp = timestamp;
        this.content = content;
    }

    public Message() {
    }

    @Override
    public String toString() {
        return "Message{" +
                "uid='" + uid + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", content='" + content + '\'' +
                '}';
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
