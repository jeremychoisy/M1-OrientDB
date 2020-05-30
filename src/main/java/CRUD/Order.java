package CRUD;

import Client.OrientDBClient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

public class Order {
    public static void main(String[] args) {
        OrientDBClient orient = new OrientDBClient();
        ODatabaseSession db = orient.getDB();

        String query = "SELECT from Order where PersonId = ? LIMIT 10";
        OResultSet rs = db.query(query, "10995116278711");

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("order: " + item.getProperty("Orderline"));
        }

        rs.close();
        orient.close();
    }
}
