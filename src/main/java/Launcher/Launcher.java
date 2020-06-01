package Launcher;

import CRUD.*;
import Client.OrientDBClient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import org.json.JSONObject;

import java.util.ArrayList;


public class Launcher {

    public static void main(String[] args) {
        // Comment the next line and uncomment the three next ones if you want to regenerate the db (will take a very long time).
        OrientDBClient orient = new OrientDBClient(false);
//        OrientDBClient orient = new OrientDBClient(true);
//        orient.createSchema();
//        orient.importData();

        ODatabaseSession db = orient.getDB();

        // CRUD
        // Person
        Person p = new Person(db);
        String pRid = p.create("John", "Doe", "male", "1981-03-21", "127.0.0.1", "customId", "1263", "Chrome");
        System.out.println("Person successfully created: ");
        p.read(pRid);
        p.update(pRid,"Johnny", "", "", "", "", "", "", "Firefox");
        System.out.println("Person updated with : firstName -> Johnny and browserUsed -> Firefox");
        p.read(pRid);
        p.delete(pRid);
        System.out.println("Person deleted.");

        // Product
        Product pr = new Product(db);
        String prRid = pr.create("www.fake-url.com", "100.0", "fake-asin", "description");
        System.out.println("Product successfully created: ");
        pr.read(prRid);
        pr.update(prRid, "", "10.0", "another-fake-asin", "");
        System.out.println("Product updated with : price -> 10.0 and asin -> another-fake-asin");
        pr.read(prRid);
        pr.delete(prRid);
        System.out.println("Product deleted.");

        // Vendor
        Vendor v = new Vendor(db);
        String vRid = v.create("fake-Brand", "fake-industry", "fake-country");
        System.out.println("Vendor successfully created: ");
        v.read(vRid);
        v.update(vRid, "another-fake-Brand", "", "another-fake-country");
        System.out.println("Vendor updated with : Vendor -> another-fake-Brand and Country -> another-fake-country");
        v.read(vRid);
        v.delete(vRid);
        System.out.println("Vendor deleted.");

        // Feedback
        Feedback f = new Feedback(db);
        String fRid = f.create("B003D9RBMU", "2199023259985", "wtf");
        if (!fRid.equals("")) {
            System.out.println("Feedback successfully created: ");
            f.read(fRid);
            f.update(fRid, "", "", "another-feedback");
            System.out.println("Feedback updated with : feedback -> another-feedback");
            f.read(fRid);
            f.delete(fRid);
            System.out.println("Feedback deleted.");
        } else {
            System.out.println("Feedback creation failed : Product or Person not found");
        }

        String orderLine = "{\"Orderline\": [" +
                "{\"productId\":\"6465\",\"asin\":\"B000FIE4WC\",\"title\":\"Topeak Dual Touch Bike Storage Stand\",\"price\":199.95,\"brand\":\"MYLAPS_Sports_Timing\"}," +
                "{\"productId\":\"178\",\"asin\":\"B002Q6DB7A\",\"title\":\"Radians Eclipse RXT Photochromic Lens with Black Frame Glass\",\"price\":61.99,\"brand\":\"Elfin_Sports_Cars\"}," +
                "{\"productId\":\"6427\",\"asin\":\"B000SE9LDK\",\"title\":\"Sportlock Leatherlock Series Deluxe Take-Down Shotgun Case\",\"price\":84.99,\"brand\":\"MYLAPS_Sports_Timing\"}," +
                "{\"productId\":\"7570\",\"asin\":\"B005G2G2OU\",\"title\":\"ESEE-5 Serr Olive Drab Textured Poweder Coated Blade Drop Point Style 1095 Carbon Steel-57 Rc\",\"price\":172.95,\"brand\":\"Derbi\"}," +
                "{\"productId\":\"1991\",\"asin\":\"B00245TWWG\",\"title\":\"Marcy Classic MD 859P Mid Size Bench\",\"price\":204.0,\"brand\":\"CCM_(ice_hockey)\"}" +
                "]}";
        JSONObject j = new JSONObject(orderLine);

        // Order
        Order o = new Order(db);
        String oRid = o.create("2199023259985",  "112.0", "fake-order-id", "2022-09-01", j.getJSONArray("Orderline").toList());
        if (!oRid.equals("")) {
            System.out.println("Order successfully created: ");
            o.read(oRid);
            o.update(oRid, "", "50.0", "", "", new ArrayList<>());
            System.out.println("Order updated with : TotalPrice -> 50.0");
            o.read(oRid);
            o.delete(oRid);
            System.out.println("Order deleted.");
        } else {
            System.out.println("Order creation failed : Person not found");
        }

        // Order
        Invoice i = new Invoice(db);
        String iRid = i.create("2199023259985",  "112.0", "fake-order-id", "2022-09-01", j.getJSONArray("Orderline").toList());
        if (!iRid.equals("")) {
            System.out.println("Invoice successfully created: ");
            i.read(iRid);
            String newOrderLine = "{\"Orderline\": [" +
                    "{\"productId\":\"6465\",\"asin\":\"B000FIE4WC\",\"title\":\"Topeak Dual Touch Bike Storage Stand\",\"price\":199.95,\"brand\":\"MYLAPS_Sports_Timing\"}," +
                    "{\"productId\":\"1991\",\"asin\":\"B00245TWWG\",\"title\":\"Marcy Classic MD 859P Mid Size Bench\",\"price\":204.0,\"brand\":\"CCM_(ice_hockey)\"}" +
                    "]}";
            JSONObject newJ = new JSONObject(newOrderLine);
            i.update(iRid, "", "", "", "2000-08-01", newJ.getJSONArray("Orderline").toList());
            System.out.println("Invoice updated with : OrderDate -> 2000-08-01 and removal of three products");
            i.read(iRid);
            i.delete(iRid);
            System.out.println("Invoice deleted.");
        } else {
            System.out.println("Invoice creation failed : Person not found");
        }

        orient.close();
    }
}
