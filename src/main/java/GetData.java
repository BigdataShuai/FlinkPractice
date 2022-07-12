import java.sql.*;

public class GetData {
    public static void main(String[] args) throws SQLException {
        //连接数据库
        Connection conn = DriverManager.getConnection("jdbc:clickhouse://node01:8123/test", "default", null);
        //抒写sql
        PreparedStatement ps = conn.prepareStatement("select * from person");
        //执行查询操作
        ResultSet result = ps.executeQuery();
        //遍历
        while (result.next()){
            int id = result.getInt(1);
            String name = result.getString(2);
            int age = result.getInt(3);
            //打印输出
            System.out.println(id + "\t" + name + "\t" + age);
        }
        //断开数据库连接
        ps.close();
        conn.close();
    }
}
