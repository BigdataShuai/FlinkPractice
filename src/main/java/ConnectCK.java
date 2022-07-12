import java.sql.*;

public class ConnectCK {
    public static void main(String[] args) throws SQLException {
        //连接数据库
        Connection conn = DriverManager.getConnection("jdbc:clickhouse://node01:8123/test", "default", null);
        //抒写sql语句
        PreparedStatement ps = conn.prepareStatement("show tables");
        //执行查询操作
        ResultSet result = ps.executeQuery();
        //遍历
        while (result.next()){
            String finResult = result.getString(1);
            //打印输出
            System.out.println(finResult);
        }
        //断开数据库连接
        ps.close();
        conn.close();
    }
}
