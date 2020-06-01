package CRUD;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Date;
import java.util.Optional;

public class Person {
    private ODatabaseSession db;

    public Person(ODatabaseSession db) {
        this.db = db;
    }

    public String create(String firstName, String lastName, String gender, String birthday, String locationIP, String id, String place, String browserUsed) {
        OVertex result = this.db.newVertex("Person");

        result.setProperty("firstName", firstName);
        result.setProperty("lastName", lastName);
        result.setProperty("gender", gender);
        result.setProperty("birthday", birthday);
        result.setProperty("locationIP", locationIP);
        result.setProperty("id", id);
        result.setProperty("place", place);
        result.setProperty("browserUsed", browserUsed);
        result.setProperty("creationDate", new Date());

        return result.save().getIdentity().toString();
    }

    public void update(String rid, String firstName, String lastName, String gender, String birthday, String locationIP, String id, String place, String browserUsed) {
        String query = "SELECT from Person where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                if (!firstName.equals("")) {
                    result.setProperty("firstName", firstName);
                }
                if (!lastName.equals("")) {
                    result.setProperty("lastName", lastName);
                }
                if (!gender.equals("")) {
                    result.setProperty("gender", gender);
                }
                if (!birthday.equals("")) {
                    result.setProperty("birthday", birthday);
                }
                if (!locationIP.equals("")) {
                    result.setProperty("locationIP", locationIP);
                }
                if (!place.equals("")) {
                    result.setProperty("place", place);
                }
                if (!browserUsed.equals("")) {
                    result.setProperty("browserUsed", browserUsed);
                }
                if (!id.equals("")) {
                    result.setProperty("id", id);
                }

                result.save();
            }
        } else {
            System.out.println("Vertex not found");
        }
    }

    public void delete(String rid) {
        String query = "SELECT from Person where @rid = ?";
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
        String query = "SELECT from Person where @rid = ?";
        OResultSet rs = db.query(query, rid);

        if (rs.hasNext()) {
            Optional<OVertex> optionalVertex = rs.next().getVertex();
            if (optionalVertex.isPresent()) {
                OVertex result = optionalVertex.get();
                System.out.println("Queried person :" + result);
            }
        } else {
            System.out.println("Vertex not found");
        }
    }
}
