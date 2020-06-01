package Launcher;

import Client.OrientDBClient;
import com.orientechnologies.orient.core.db.ODatabaseSession;


public class Launcher {

    public static void main(String[] args) {
//        OrientDBClient orient = new OrientDBClient(true);
        OrientDBClient orient = new OrientDBClient(false);
        orient.createSchema();
        orient.importData();

        ODatabaseSession db = orient.getDB();


        // TODO: add queries

        orient.close();
    }
}
