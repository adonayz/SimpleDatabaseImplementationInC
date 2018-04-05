import simpledb.remote.SimpleDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Adonay on 4/4/2018.
 */
public class Examples {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            String s = "create table BUILDING(BId int, BName varchar(10), DeptID int, Floors int)";
            stmt.executeUpdate(s);
            System.out.println("Table BUILDING created.");

            s = "insert into BUILDING(BId, BName, DeptID, Floors) values ";
            String[] bldgvals = {"(1, 'Fuller Labs', 1, 5)",
                    "(2, 'Kaven Hall', 2, 3)",
                    "(3, 'Stratton Hall', 3, 4)",
                    "(4, 'Salisbury Labs', 4, 5)",
                    "(5, 'Atwater Kent', 5, 4)",
                    "(6, 'Olin Hall', 6, 4)",
                    "(7, 'Higgins Labs', 7, 3)"};

            for (int i=0; i<bldgvals.length; i++)
                stmt.executeUpdate(s + bldgvals[i]);
            System.out.println("BUILDING records inserted.");

            s = "create table DEPARTMENT(DId int, DName varchar(25))";
            stmt.executeUpdate(s);
            System.out.println("Table DEPARTMENT created.");

            s = "insert into DEPARTMENT(DId, DName) values ";
            String[] deptvals = {"(1, 'Computer Science')",
                    "(2, 'Civil Engineering')",
                    "(3, 'Math')",
                    "(4, 'Humanities')",
                    "(5, 'Electrical Engineering')",
                    "(6, 'Physics')",
                    "(7, 'Mechanical Engineering')"};
            for (int i=0; i<deptvals.length; i++)
                stmt.executeUpdate(s + deptvals[i]);
            System.out.println("DEPARTMENT records inserted.");
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (conn != null)
                    conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
