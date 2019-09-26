package cn.ffcs;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 *  封装操作类
 */
public class SQLSourceHelper {

    private SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 在终端打印日志信息
     */
    private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);


    /**
     * 两次查询时间间隔
     */
    private int runQueryDelay;

    /**
     * 单次最大提交条数
     */
    private int maxSubmitCount;


    /**
     * 开始的时间戳
     */
    private String startFrom;

    /**
     * 上一次查询时间戳的位置
     */
    private String currentIndex;

    /**
     * 本次查询结束之后保存时间戳最大的值
     */
    private String endFrom;

    /**
     * 所要查询的表（源表）
     */
    private String table;

    /**
     * 用户传入的查询语句
     */
    private String customQuery;

    /**
     * 构建的查询语句
     */
    private String query;

    /**
     * 默认编码集
     */
    private String defaultCharsetResultSet;

    /**
     * 返回结果的分隔符
     */
    private String split_field;

    /**
     * 源表中的时间戳
     */
    private String query_time;

    /**
     *  上下文，用来获取配置文件
     */
    private Context context;



    /**
     * 单次查询提交的时间间隔的默认值
     */
    private static final int DEFAULT_QUERY_DELAY = 10000;

    /**
     * 辅助表初始时间的默认值
     */
    private static final String DEFAULT_START_VALUE = "1970-01-01 08:00:01";

    /**
     * 默认编码
     */
    private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";


    /**
     * 返回单条结果的分隔符
     */
    private static final String DEFAULT_SPLIT_FIELD = "\t";

    /**
     * 所要查询的数据库连接对象
     */
    private static Connection conn = null;
    /**
     * 辅助表所在数据库连接对象
     */
    private static Connection meta_conn = null;
    /**
     * 查询数据库的Statement接口对象
     */
    private static PreparedStatement ps = null;
    /**
     * 辅助表的Statement接口对象
     */
    private static PreparedStatement meta_ps = null;
    /**
     * 查询的数据库的url地址
     */
    private static String connectionURL;

    /**
     * 查询的数据库的用户名
     */
    private static String connectionUserName;
    /**
     * 查询的数据库的密码
     */
    private static String connectionPassword;
    /**
     * 查询的数据库的驱动
     */
    private static String connectionDriver;
    /**
     * 辅助表数据源链接
     */
    private static String meta_connectionURL;

    /**
     * 辅助表所在数据库的用户名
     */
    private static String meta_connectionUserName;

    /**
     * 辅助表所在数据库的密码
     */
    private static String meta_connectionPassword;

    /**
     * 辅助表所在数据库的驱动
     */
    private static String meta_connectionDriver;
    /**
     * 辅助表名
     */
    private static String meta_table;
    /**
     * 辅助表中存储对应查询表的名称
     */
    private static String meta_table_name;

    private static String meta_table_id;

    private static String meta_table_currentTime;

    /**
     * 辅助表对应-查询表上一次查询之后所保存的时间戳
     */
    private static String meta_table_index;
    /**
     * 如果辅助表不存在则创建，设置相应编码
     */
    private static String defaultCharsetmetatable;

    private static int time_split;

    private static String stringToDate;

    private static String des_key;


    //加载静态资源
    static {
        Properties p = new Properties();

        try {
            p.load(SQLSourceHelper.class.getClassLoader().getResourceAsStream("jdbc.properties"));
            meta_table = p.getProperty("meta_table");
            meta_table_name = p.getProperty("meta_table_name");
            meta_table_id=p.getProperty("meta_table_id");
            meta_table_currentTime=p.getProperty("meta_table_currentTime");
            meta_table_index = p.getProperty("meta_table_index");
            defaultCharsetmetatable = p.getProperty("defaultCharsetmetatable");

            meta_connectionURL = p.getProperty("meta_connection.url");
            meta_connectionUserName = p.getProperty("meta_connection.user");
            meta_connectionPassword = p.getProperty("meta_connection.password");
            Class.forName(p.getProperty("meta_connection.driver"));

            meta_conn = InitConnection(meta_connectionURL, meta_connectionUserName, meta_connectionPassword);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取数据库连接对象
     * @param url 数据库地址
     * @param user 用户名
     * @param pw 密码
     * @return 获取JDBC连接
     */
    private static Connection InitConnection(String url, String user, String pw) {

        try {

            Connection conn = DriverManager.getConnection(url, user, pw);

            if (conn == null)
                throw new SQLException();

            return conn;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 初始化构造器的值，读取相应的flume的配置文件信息
     * @param context   指定构造方法
     * @throws ParseException
     */
    SQLSourceHelper(Context context) throws ParseException {

        //初始化上下文
        this.context = context;

        //有默认值参数：获取flume任务配置文件中的参数，读不到的采用默认值

        this.runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);

        this.startFrom = context.getString("start.from", DEFAULT_START_VALUE);

        this.defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

        this.split_field = context.getString("split_field", DEFAULT_SPLIT_FIELD);

        this.time_split = context.getInteger("time_split");

        this.query_time = context.getString("query_time");

        this.maxSubmitCount = context.getInteger("maxSubmitCount");

        des_key = context.getString("des_key");

        //无默认值参数：获取flume任务配置文件中的参数
        this.table = context.getString("table");

        this.customQuery = context.getString("custom.query", DEFAULT_START_VALUE);


        try {
            Class.forName(context.getString("connection.driver"));

            connectionURL = context.getString("connection.url");

            connectionUserName = context.getString("connection.user");

            connectionPassword = context.getString("connection.password");

            if(connectionURL.contains("mysql")){
                stringToDate = "STR_TO_DATE";
            }else{
                stringToDate = "TO_DATE";
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            check_password(des_key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        conn = InitConnection(connectionURL, connectionUserName, connectionPassword);

        //校验相应的配置信息，如果没有默认值的参数也没赋值，抛出异常
        checkMandatoryProperties();

        String sql = "CREATE TABLE IF NOT EXISTS `" + meta_table + "` (`" + meta_table_name + "` varchar(40) NOT NULL,\n" +
                "\t`" + meta_table_index + "` timestamp default '1970-01-01 08:00:01',\n" +
                "\tPRIMARY KEY (`" + meta_table_name + "`)) ENGINE=InnoDB DEFAULT CHARSET='" + defaultCharsetmetatable + "'";
        execSql(meta_conn,sql,meta_ps);

        //获取当前的id
        currentIndex = getStatusDBIndex(startFrom);

        //构建查询语句
        query = buildQuery();

    }

    private void check_password(String des_key) throws Exception {
        if(connectionPassword.contains(table)){
            //如果当前是加密过的则进行解密
            connectionPassword=connectionPassword.substring(table.length());
            connectionPassword=DESUtil.decrypt(connectionPassword, des_key);
        }else{
            //如果当前是明文密码则进行加密
            Log.info("请设置密钥为："+table+DESUtil.encrypt(connectionPassword,des_key ));
        }
    }


    /**
     * 校验相应的配置信息（表，查询语句以及数据库连接的参数）
     */
    private void checkMandatoryProperties() {

        if (table == null) {
            throw new ConfigurationException("property table not set");
        }

        if (connectionURL == null) {
            throw new ConfigurationException("connection.url property not set");
        }

        if (connectionUserName == null) {
            throw new ConfigurationException("connection.user property not set");
        }

        if (connectionPassword == null) {
            throw new ConfigurationException("connection.password property not set");
        }
    }


    /**
     * 构建sql语句
     * @return 返回相应的sql语句
     */
    private String buildQuery() {

        String sql = "";

        //获取当前时间戳
        currentIndex = getStatusDBIndex(startFrom);
        LOG.info(currentIndex + "");


        sql = customQuery;


        StringBuilder execSql = new StringBuilder(sql);
        long addTimeMills = 0;
        //不设置时间间隔的话默认取到当前

        if(time_split == -1){
            this.endFrom = sf.format(new Date());
        }else{
            try {
                //60000是60秒钟
                endFrom = sf.format(new Date());
                addTimeMills = sf.parse(currentIndex).getTime()+60000*time_split;
                String nowTime = sf.format(new Date(addTimeMills));
                endFrom = endFrom.compareTo(nowTime) > 0?nowTime:endFrom;
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        //以时间戳作为offset
        if (!sql.contains("where")) {
            execSql.append(" where ");
//            execSql.append(query_time).append(" < ")
//                    .append(stringToDate).append("('").append(endFrom)
//                    .append("','%Y-%m-%d %H:%i:%S') and ")
//                    .append(query_time).append(" > ")
//                    .append(stringToDate).append("('")
//                    .append(currentIndex).append("','%Y-%m-%d %H:%i:%S')");
            execSql.append(query_time).append(" < '")
                    .append(endFrom+"'")
                    .append(" and ")
                    .append(query_time).append(" > '")
                    .append(currentIndex+"'");

            return execSql.toString();
        } else {
            int length = execSql.toString().length();
            while (execSql.charAt(length - 1) != '<')
                length--;

//            return execSql.toString().substring(0, length) + stringToDate+"('" +
//                    endFrom + "','%Y-%m-%d %H:%i:%S') and "+query_time+
//                    " > "+stringToDate+"('"+ currentIndex + "','%Y-%m-%d %H:%i:%S')";
            return execSql.toString().substring(0, length) +
                    "'"+endFrom + "' and "+query_time+
                    " > '"+ currentIndex+"'";
        }
    }


    /**
     * 执行查询语句
     * @return 以集合形式返回查询结果，集合中每个元素是一条结果，通过list存放是因为要拼接该结果
     */
    List<List<Object>> executeQuery() {

        try {
            //每次执行查询时都要重新生成sql，因为id不同
            customQuery = buildQuery();

            //存放结果的集合
            List<List<Object>> results = new ArrayList<>();

            if (ps == null) {
                //
                ps = conn.prepareStatement(customQuery);
            }

            ResultSet result = ps.executeQuery(customQuery);

            while (result.next()) {

                //存放一条数据的集合（多个列）
                List<Object> row = new ArrayList<>();

                //将返回结果放入集合
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                    row.add(result.getObject(i));
                }

                results.add(row);
            }

            LOG.info("execSql:" + customQuery + "\nresultSize:" + results.size());

            return results;
        } catch (SQLException e) {
            LOG.error(e.toString());

            // 重新连接
            conn = InitConnection(connectionURL, connectionUserName, connectionPassword);

        }

        return null;
    }


    /**
     * @param queryResult 输入查询结果集
     * @return 将结果集转化为字符串，每一条数据是一个list集合，将每一个小的list集合转化为字符串
     */
    List<String> getAllRows(List<List<Object>> queryResult) {

        List<String> allRows = new ArrayList<>();

        if (queryResult == null || queryResult.isEmpty())
            return allRows;

        StringBuilder row = new StringBuilder();

        for (List<Object> rawRow : queryResult) {

            Object value = null;

            for (Object aRawRow : rawRow) {

                value = aRawRow;

                if (value == null) {
                    row.append(split_field);
                } else {
                    row.append(aRawRow.toString()).append(split_field);
                }
            }

            allRows.add(row.toString().substring(0, row.length()-1));
            row = new StringBuilder();
        }

        return allRows;
    }


    /**
     * 更新meta辅助表状态，每次返回结果集后调用。必须记录每次查询的offset值，为程序中断续跑数据时使用，以时间戳为offset
     */
    void updateOffset2DB() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = sf.format(new Date());
        //以source_tab做为KEY，如果不存在则插入，存在则更新（每个源表对应一条记录）
        String sql = "insert into " + meta_table + "(" + meta_table_name + "," + meta_table_index + ","+meta_table_currentTime+") VALUES('" +
                table + "','" + endFrom + "','"+currentTime+"') on DUPLICATE key update " + meta_table_name + "=values(" + meta_table_name
                + ")," + meta_table_index + "=values(" + meta_table_index + ")";

        LOG.info("updateStatus Sql:" + sql);

        execSql(meta_conn,sql,meta_ps);
    }


    /**
     * 执行sql语句
     * @param conn 指定操作的数据源
     * @param sql   指定执行sql语句
     * @param ps 相应数据库执行命令
     */
    private void execSql(Connection conn,String sql,PreparedStatement ps) {

        try {
            ps = conn.prepareStatement(sql);

            LOG.info("exec::" + sql);

            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 查找辅助表中的时间戳offset，如果没有则为初始时间
     * @param startFrom 如果没有数据，则说明是第一次查询或者数据表中还没有存入数据，返回最初传入的值
     * @return 获取当前时间戳的offset
     */
    private String getStatusDBIndex(String startFrom) {

        //从flume_meta表中查询出当前的时间戳是多少
        String dbIndex = queryOne("select " + meta_table_index
                + " from " + meta_table + " where " + meta_table_name + "='" + table + "'",meta_ps);

        if (dbIndex != null) {
            return dbIndex;
        }

        //如果没有数据，则说明是第一次查询或者数据表中还没有存入数据，返回最初传入的值
        return startFrom;
    }


    /**
     * 查找辅助表中时间戳
     * @param sql 执行查找辅助表的sql
     * @param ps  相应操作的数据源
     * @return 返回一条数据的执行语句(当前时间戳)
     */
    private String queryOne(String sql,PreparedStatement ps) {

        ResultSet result = null;

        try {
            ps = meta_conn.prepareStatement(sql);
            result = ps.executeQuery();

            while (result.next()) {
                return result.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * 关闭相应资源
     */
    void close() {

        try {
            ps.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * @return 封装两次查询时间间隔
     */
    int getRunQueryDelay() {
        return runQueryDelay;
    }

    /**
     * @return 封装单次提交到channel的最大条数
     */
    public int getMaxSubmitCount() {
        return maxSubmitCount;
    }

}
