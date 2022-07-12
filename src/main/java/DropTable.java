import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DropTable {
    public static void main(String[] args) throws SQLException {
        //连接数据库
        Connection conn = DriverManager.getConnection("jdbc:clickhouse://node01:8123/test", "default", null);
        //抒写sql
        PreparedStatement ps = conn.prepareStatement("drop table person");
        //执行操作
        boolean result = ps.execute();
        //判断
        if(result==false){
            System.out.println("删除成功！！！");
        }else{
            System.out.println("删除失败！！！");
        }
    }
}
