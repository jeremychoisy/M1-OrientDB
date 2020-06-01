package Client;

import Loader.DataLoader;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Optional;

//import java.util.ArrayList;

public class OrientDBClient {
    private OrientDB orient;
    private ODatabaseSession db;
    private DataLoader dataLoader;

    private static final String[] VERTEX_CLASSES = {"Person", "Feedback", "Order", "Tag", "Post", "Invoice", "Vendor", "Product"};
    private static final String[] EDGE_CLASSES = {"VendorSellProduct", "PersonHasInterestInTag", "PersonKnowsPerson", "PostWasCreatedByPerson", "PostIsLabelledByTag",
            "ProductIsInOrder", "ProductHasFeedback", "PersonGaveFeedback", "PersonBoughtOrder", "OrderHasInvoice"};

    public OrientDBClient(boolean recreateDB) {
        this.orient = new OrientDB("remote:88.136.56.208", "root", "root", OrientDBConfig.defaultConfig());
        if (recreateDB) {
            recreateDB();
        }
        this.db = this.orient.open("big-data-project", "admin", "admin");
        this.dataLoader = new DataLoader(this.db);
    }

    private void recreateDB() {
        System.out.println("Recreating db...");
        this.orient.drop("big-data-project");
        this.orient.create("big-data-project", ODatabaseType.PLOCAL);
        System.out.println("DB has been successfully created.");
    }

    public void close() {
        this.orient.close();
        this.db.close();
    }

    public ODatabaseSession getDB() {
        return this.db;
    }

    public void createSchema() {
        System.out.println("Creating schema...");
        for (String vertexClass : VERTEX_CLASSES) {
            if (db.getClass(vertexClass) == null) {
                db.createVertexClass(vertexClass);
            }
        }

        for (String edgeClass : EDGE_CLASSES) {
            if (db.getClass(edgeClass) == null) {
                db.createEdgeClass(edgeClass);
            }
        }

        // Indexes
        if (db.getClass("Person").getProperty("id") == null) {
            db.getClass("Person").createProperty("id", OType.STRING);
            db.getClass("Person").createIndex("Person_id_index", OClass.INDEX_TYPE.UNIQUE, "id");
        }
        if (db.getClass("Order").getProperty("OrderId") == null) {
            db.getClass("Order").createProperty("OrderId", OType.STRING);
            db.getClass("Order").createIndex("Order_id_index", OClass.INDEX_TYPE.UNIQUE, "OrderId");
        }
        if (db.getClass("Product").getProperty("asin") == null) {
            db.getClass("Product").createProperty("asin", OType.STRING);
            db.getClass("Product").createIndex("Product_asin_index", OClass.INDEX_TYPE.UNIQUE, "asin");
        }
        if (db.getClass("Vendor").getProperty("Vendor") == null) {
            db.getClass("Vendor").createProperty("Vendor", OType.STRING);
            db.getClass("Vendor").createIndex("Vendor_Vendor_index", OClass.INDEX_TYPE.UNIQUE, "Vendor");
        }
        if (db.getClass("Post").getProperty("id") == null) {
            db.getClass("Post").createProperty("id", OType.STRING);
            db.getClass("Post").createIndex("Post_id_index", OClass.INDEX_TYPE.UNIQUE, "id");
        }
        System.out.println("Schema has been successfully created.");
    }

    public void importData() {
        System.out.println("Generating data...");
        // Vertex
//        ArrayList<String> arrayNodeNames = new ArrayList<>();
//        arrayNodeNames.add("Orderline");
//        this.dataLoader.loadXMLIntoDB("invoice.xml", "Invoice", arrayNodeNames);
//        this.dataLoader.loadCSVIntoDB("person.csv", "\\|", "Person");
//        this.dataLoader.loadCSVIntoDB("feedback.csv", "\\|", "Feedback");
//        this.dataLoader.loadCSVIntoDB("post.csv", "\\|", "Post");
//        this.dataLoader.loadCSVIntoDB("product.csv", ",", "Product");
//        this.dataLoader.loadCSVIntoDB("vendor.csv", ",", "Vendor");
//        this.dataLoader.loadJSONIntoDB("order.json", "Order");

        // Imported Edge
//        this.dataLoader.loadCSVEdgeIntoDB("brandByProduct.csv", ",", "VendorSellProduct");
//        this.dataLoader.loadCSVEdgeIntoDB("person_hasInterest_tag.csv", "\\|", "PersonHasInterestInTag");
//        this.dataLoader.loadCSVEdgeIntoDB("person_knows_person.csv", "\\|", "PersonKnowsPerson");
//        this.dataLoader.loadCSVEdgeIntoDB("post_hasCreator_person.csv", "\\|", "PostWasCreatedByPerson");
//        this.dataLoader.loadCSVEdgeIntoDB("post_hasTag_tag.csv", "\\|", "PostIsLabelledByTag");

        // Generated Edge
        generateEdge("ProductIsInOrder", "Product", "Order", "asin", "Orderline.asin", true);
//        generateEdge("ProductHasFeedback", "Product", "Feedback", "asin", "productId", false);
//        generateEdge("PersonGaveFeedback", "Person", "Feedback", "id", "personId", false);
//        generateEdge("PersonBoughtOrder", "Person", "Order", "id", "PersonId", false);
//        generateEdge("OrderHasInvoice", "Order", "Invoice", "OrderId", "OrderId", false);

        System.out.println("Data has been successfully generated.");
    }

    private void generateEdge(String className, String outClass, String inClass, String outField, String inField, boolean isInFieldQueriedInArray) {
        String firstQuery = "SELECT  * from " + outClass;
        OResultSet firstRs = db.query(firstQuery);

        while (firstRs.hasNext()) {
            OResult firstRes = firstRs.next();
            String index = firstRes.getProperty(outField);

            String secondQuery;
            if (isInFieldQueriedInArray) {
                String[] fields = inField.split(".");
                secondQuery = "SELECT from " + inClass + " where " + fields[0] + " CONTAINS( " + fields[1] + " = ?)";
            } else {
                secondQuery = "SELECT from " + inClass + " where " + inField + " = ?";
            }
            OResultSet secondRs = db.query(secondQuery, index);

            Optional<OVertex> optionalFirstVertex = firstRes.getVertex();

            while(secondRs.hasNext()) {
                Optional<OVertex> optionalSecondVertex = secondRs.next().getVertex();

                if (optionalFirstVertex.isPresent() && optionalSecondVertex.isPresent()) {
                    OVertex firstVertex = optionalFirstVertex.get();
                    OVertex secondVertex = optionalSecondVertex.get();
                    OEdge edge = firstVertex.addEdge(secondVertex, className);

                    edge.save();
                }
            }
            secondRs.close();
        }
        firstRs.close();
    }
}
