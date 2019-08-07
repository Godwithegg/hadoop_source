package com.danhuang.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHbase {

    private static Admin admin = null;
    private static Connection conn = null;
    private static Configuration configuration = null;
    static{
        configuration = configuration = HBaseConfiguration.create();
        //Hbase配置文件
//        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum","192.168.160.102");
//        configuration.set("hbase.zookeeper.property.clientPort","2181");

        //获取连接对象
        conn = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }


        //获取Hbase管理员对象
//        HBaseAdmin admin = new HBaseAdmin(configuration);
    }

    private static void close(Connection conn,Admin admin){
        if(conn!=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //判断表是否存在
    public static boolean tableExist(String tableName) throws IOException {

        boolean result = admin.tableExists(TableName.valueOf(tableName));

        //执行
//        boolean result = admin.tableExists(tableName);

        //关闭资源
        admin.close();
        return result;
    }

    //创建表
    public static void createTable(String tableName,String... cfs) throws IOException {

        if(tableExist(tableName)){
            System.out.println("表已存在!!");
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //添加列族
        for (String cf : cfs) {
            //创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
//            hColumnDescriptor.setMaxVersions(1);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //创建表操作
        admin.createTable(hTableDescriptor);
        System.out.println("表创建成功");
    }

    //删除表
    public static void deleteTable(String tableName) throws IOException {

        //使表不可用
        admin.disableTable(TableName.valueOf(tableName));

        //执行删除操作
        admin.deleteTable(TableName.valueOf(tableName));

        System.out.println("表已删除！！");
    }

    //增//改
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {

        //获取表对象
        Table table = conn.getTable(TableName.valueOf(tableName));

        //获取表对象
//        HTable table = new HTable(configuration, TableName.valueOf(tableName));

        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //添加数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        //执行添加操作
        table.put(put);
        table.close();

    }

    //删
    public static void delete(String tableName,String rowKey,String cf,String cn) throws IOException {

        //获取Table对象
        Table table = conn.getTable(TableName.valueOf(tableName));

        //创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //删除所有版本
//        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //执行删除操作
        table.delete(delete);

        //关闭资源
        table.close();
    }

    //查
    //全表扫描
    public static void scanTable(String tableName) throws IOException {

        //获取table对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //构建扫描器
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        //遍历数据并打印
        for(Result result:resultScanner){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))
                +",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                +",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                +",VALUE:"+Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }
        table.close();
    }

    //获取指定列族，列的数据
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {

        //获取table对象
        Table table = conn.getTable(TableName.valueOf(tableName));

        //创建一个get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
//        get.setMaxVersions();

        //获取数据的操作
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))
                    +",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +",VALUE:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    public static void main(String[] args) throws IOException {

//        System.out.println(tableExist("student"));
//        System.out.println(tableExist("staff"));
//
//        createTable("staff","info");
//        System.out.println(tableExist("staff"));
//
//        deleteTable("staff");

//        putData("student","1001","info","sex","female");

//        delete("student","1001","info","sex");
//        scanTable("student");
//        System.out.println(tableExist("staff"));
        getData("student", "1001","info" ,"info" );
        close(conn,admin);
    }
}
