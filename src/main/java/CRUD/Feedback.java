package CRUD;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Optional;

public class Feedback {
    private ODatabaseSession db;

    public Feedback(ODatabaseSession db) {
        this.db = db;
    }

    public String create(String productId, String personId, String feedback) {
        String firstQuery = "SELECT from Product where asin = ?";
        OResultSet firstRs = db.query(firstQuery, productId);
        String secondQuery = "SELECT from Person where id = ?";
        OResultSet secondRs = db.query(secondQuery, personId);

        if (firstRs.hasNext() && secondRs.hasNext()) {
            OVertex result = this.db.newVertex("Feedback");

            result.setProperty("productId", productId);
            result.setProperty("personId", personId);
            result.setProperty("feedback", feedback);

            return result.save().getIdentity().toString();
        }

        return "";
    }

    public void update(String rid, String productId, String personId, String feedback) {
        String query = "SELECT from Feedback where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                if (!productId.equals("")) {
                    String productQuery = "SELECT from Product where asin = ?";
                    OResultSet productRs = db.query(productQuery, productId);
                    if (productRs.hasNext()) {
                        result.setProperty("productId", productId);
                    } else {
                        System.out.println("Update failed : Product not found");
                        return;
                    }
                }
                if (!personId.equals("")) {
                    String personQuery = "SELECT from Person where id = ?";
                    OResultSet personRs = db.query(personQuery, personId);
                    if (personRs.hasNext()) {
                        result.setProperty("personId", personId);
                    } else {
                        System.out.println("Update failed : Person not found");
                        return;
                    }
                }
                if (!feedback.equals("")) {
                    result.setProperty("feedback", feedback);
                }

                result.save();
            }
        } else {
            System.out.println("Vertex not found");
        }
    }

    public void delete(String rid) {
        String query = "SELECT from Feedback where @rid = ?";
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
        String query = "SELECT from Feedback where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                System.out.println("Queried feedback :" + result);
            }
        } else {
            System.out.println("Vertex not found");
        }
    }
}
