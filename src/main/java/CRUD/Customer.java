package CRUD;

import Client.OrientDBClient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

public class Customer {
    // Test
    public static void main(String[] args) {
        OrientDBClient orient = new OrientDBClient();
        ODatabaseSession db = orient.getDB();
        String query = "SELECT from Person where lastName = ?";
        OResultSet rs = db.query(query, "Chen");

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("friend: " + item.getProperty("firstName"));
        }

        rs.close();
        orient.close();
    }
}
