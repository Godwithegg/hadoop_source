package com.danhuang;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class TestHbase {

    //判断表是否存在
    public static boolean tableExist(String tableName) throws IOException {

        //Hbase配置文件
        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum","192.168.160.102");
//        configuration.set("hbase.zookeeper.property.clientPort","2181");

        //获取Hbase管理员对象
        HBaseAdmin admin = new HBaseAdmin(configuration);

        //执行
        boolean result = admin.tableExists(tableName);

        //关闭资源
        admin.close();
        return result;
    }

    //创建表

    //删除表

    //增//改

    //删

    //查

    public static void main(String[] args) throws IOException {

        System.out.println(tableExist("student"));
        System.out.println(tableExist("staff"));
    }
}
