import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    public static void main(String[] args) throws ParseException {

        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test_flume", "root", "000000");
            for(int i=0;i<100000;i++){
                PreparedStatement ps = conn.prepareStatement("insert into teacher(name,age) value('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)" +
                        ",('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12),('a',12)");
                ps.execute();
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
