package CRUD;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.List;
import java.util.Optional;

public class Invoice {
    private ODatabaseSession db;

    public Invoice(ODatabaseSession db) {
        this.db = db;
    }

    public String create(String PersonId, String TotalPrice, String OrderId, String OrderDate, List<Object> orderLine) {
        String query = "SELECT from Person where id = ?";
        OResultSet rs = db.query(query, PersonId);

        if (rs.hasNext()) {
            OVertex result = this.db.newVertex("Invoice");

            result.setProperty("PersonId", PersonId);
            result.setProperty("TotalPrice", TotalPrice);
            result.setProperty("OrderId", OrderId);
            result.setProperty("OrderDate", OrderDate);
            result.setProperty("orderline", orderLine);

            return result.save().getIdentity().toString();
        }
        return "";
    }

    public void update(String rid, String PersonId, String TotalPrice, String OrderId, String OrderDate, List<Object> orderLine) {
        String query = "SELECT from Invoice where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                if (!PersonId.equals("")) {
                    result.setProperty("PersonId", PersonId);
                }
                if (!TotalPrice.equals("")) {
                    result.setProperty("TotalPrice", TotalPrice);
                }
                if (!OrderId.equals("")) {
                    result.setProperty("OrderId", OrderId);
                }
                if (!OrderDate.equals("")) {
                    result.setProperty("OrderDate", OrderDate);
                }
                if (orderLine.size() > 0) {
                    result.setProperty("orderline", orderLine);
                }

                result.save();
            }
        } else {
            System.out.println("Vertex not found");
        }
    }

    public void delete(String rid) {
        String query = "SELECT from Invoice where @rid = ?";
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
        String query = "SELECT from Invoice where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                System.out.println("Queried invoice :" + result);
            }
        } else {
            System.out.println("Vertex not found");
        }
    }
}
