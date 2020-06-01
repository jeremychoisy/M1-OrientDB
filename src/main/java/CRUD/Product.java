package CRUD;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Optional;

public class Product {
    private ODatabaseSession db;

    public Product(ODatabaseSession db) {
        this.db = db;
    }

    public String create(String imgUrl, String price, String asin, String title) {
        OVertex result = this.db.newVertex("Product");

        result.setProperty("imgUrl", imgUrl);
        result.setProperty("price", price);
        result.setProperty("asin", asin);
        result.setProperty("title", title);

        return result.save().getIdentity().toString();
    }

    public void update(String rid, String imgUrl, String price, String asin, String title) {
        String query = "SELECT from Product where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                if (!imgUrl.equals("")) {
                    result.setProperty("imgUrl", imgUrl);
                }
                if (!price.equals("")) {
                    result.setProperty("price", price);
                }
                if (!title.equals("")) {
                    result.setProperty("title", title);
                }
                if (!asin.equals("")) {
                    result.setProperty("asin", asin);
                }

                result.save();
            }
        } else {
            System.out.println("Vertex not found");
        }
    }

    public void delete(String rid) {
        String query = "SELECT from Product where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                optionalVertex.get().delete();
            }
        } else {
            System.out.println("Vertex not found");
        }
    }

    public void read(String rid) {
        String query = "SELECT from Product where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                System.out.println("Queried product :" + result);
            }
        } else {
            System.out.println("Vertex not found");
        }
    }
}
