package com.danhuang.weibo;

import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;

public class Weibo {

    public static void init() throws IOException {
        //创建命名空间
        WeiBoUtil.createNamespace(Constant.NAMESPACE);
        //创建内容表
        WeiBoUtil.createTable(
                Constant.CONTENT,
                1,
                "info");
        //创建用户关系表
        WeiBoUtil.createTable(Constant.RELATIONS,
                1,
                "attends",
                "fans");

        //创建收件箱表（多版本）
        WeiBoUtil.createTable(Constant.INBOX,
                2,
                "info");
    }

    public static void main(String[] args) throws IOException {

        //测试
//        init();

        //1001，1002发布微博
//        WeiBoUtil.createData("1001","今天天气真好！" );
//        WeiBoUtil.createData("1002","今天天气不好！" );

        //1001关注1002和1003
//        WeiBoUtil.addAttend("1001","1002","1003");

        //获取1001初始化页面信息
        WeiBoUtil.getInit("1001");
        System.out.println();

        //1003发布微博
//        WeiBoUtil.createData("1003", "今天被开罚单了");
//        WeiBoUtil.createData("1003", "今天想学习");
//        WeiBoUtil.createData("1003", "明天想学习");

        //获取1001初始化页面信息
        WeiBoUtil.getInit("1001");
        //取关
        WeiBoUtil.deleteAttend("1001","1002");
        System.out.println();
        //获取1001初始化页面信息
        WeiBoUtil.getInit("1001");
    }
}
