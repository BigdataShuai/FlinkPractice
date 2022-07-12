import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertCK {
    public static void main(String[] args) throws SQLException {
        //连接数据库
        Connection conn = DriverManager.getConnection("jdbc:clickhouse://node01:8123/test", "default", null);
        //抒写sql
        PreparedStatement ps = conn.prepareStatement("insert into person values(?,?,?)");
        //加载数据
        Object[][] data = {
                {1, "zhangsan", 13},
                {2, "lisi", 14},
                {3, "wangwu", 15},
                {4, "zhaoliu", 16},
                {5, "zhouqi", 17}};
        //使用增强for遍历二维数组
        for (Object[] datum : data) {
            //遍历数组当中的元素
            for (int i = 0; i < datum.length; i++) {
                //将数据保存到表中
                ps.setObject(i+1,datum[i]);
            }
            //执行操作
            ps.execute();
        }
        //断开数据库连接
        ps.close();
        conn.close();
    }
}
