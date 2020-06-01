package CRUD;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Optional;

public class Vendor {
    private ODatabaseSession db;

    public Vendor(ODatabaseSession db) {
        this.db = db;
    }

    public String create(String Vendor, String Industry, String Country) {
        OVertex result = this.db.newVertex("Vendor");

        result.setProperty("Vendor", Vendor);
        result.setProperty("Industry", Industry);
        result.setProperty("Country", Country);

        return result.save().getIdentity().toString();
    }

    public void update(String rid, String Vendor, String Industry, String Country) {
        String query = "SELECT from Vendor where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                if (!Industry.equals("")) {
                    result.setProperty("Industry", Industry);
                }
                if (!Country.equals("")) {
                    result.setProperty("Country", Country);
                }
                if (!Vendor.equals("")) {
                    result.setProperty("Vendor", Vendor);
                }

                result.save();
            }
        } else {
            System.out.println("Vertex not found");
        }
    }

    public void delete(String rid) {
        String query = "SELECT from Vendor where @rid = ?";
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
        String query = "SELECT from Vendor where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                System.out.println("Queried vendor :" + result);
            }
        } else {
            System.out.println("Vertex not found");
        }
    }
}
