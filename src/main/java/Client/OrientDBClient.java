package Client;

import Loader.DataLoader;
import com.orientechnologies.orient.core.db.ODatabaseSession;
//import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.util.ArrayList;

public class OrientDBClient {
    private OrientDB orient;
    private ODatabaseSession db;
    private DataLoader dataLoader;
    private static final String[] VERTEX_CLASSES = {"Person", "Feedback", "Order", "Tag", "Post", "Invoice", "Seller", "Product"};

    public OrientDBClient(){
        this.orient = new OrientDB("remote:88.136.56.208", "root", "root", OrientDBConfig.defaultConfig());
//        this.orient.drop("big-data-project");
//        this.orient.create("big-data-project", ODatabaseType.PLOCAL);
        this.db = this.orient.open("big-data-project", "admin", "admin");
        this.dataLoader = new DataLoader(this.db);
//        createSchema();
//        importData();
    }

    public void close(){
        this.orient.close();
        this.db.close();
    }

    public ODatabaseSession getDB() {
        return this.db;
    }

    private void createSchema() {
        for (String vertexClass : VERTEX_CLASSES) {
            if (db.getClass(vertexClass) == null) {
                db.createVertexClass(vertexClass);
            }
        }

        if (db.getClass("Person").getProperty("id") == null) {
            db.getClass("Person").createProperty("id", OType.STRING);
            db.getClass("Person").createIndex("Person_id_index", OClass.INDEX_TYPE.UNIQUE, "id");
        }
        if (db.getClass("Order").getProperty("OrderId") == null) {
            db.getClass("Order").createProperty("OrderId", OType.STRING);
            db.getClass("Order").createIndex("Order_id_index", OClass.INDEX_TYPE.UNIQUE, "OrderId");
        }
    }

    private void importData() {
        ArrayList<String> arrayNodeNames = new ArrayList<>();
        arrayNodeNames.add("Orderline");
        this.dataLoader.loadXMLIntoDB("invoice.xml", "Invoice", arrayNodeNames);
        this.dataLoader.loadCSVIntoDB("person.csv", "\\|", "Person");
        this.dataLoader.loadCSVIntoDB("feedback.csv", "\\|", "Feedback");
        this.dataLoader.loadJSONIntoDB("order.json","Order");
    }
}
