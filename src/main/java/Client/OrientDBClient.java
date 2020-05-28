package Client;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public class OrientDBClient {
    private OrientDB orient;
    private ODatabaseSession db;

    public OrientDBClient(){
        this.orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        this.db = this.orient.open("big-data-project", "root", "root");
    }

    public void close(){
        this.orient.close();
        this.db.close();
    }

    public ODatabaseSession getDB() {
        return this.db;
    }
}
