package Loaders;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;
import org.json.JSONObject;
import java.io.*;
import java.nio.charset.StandardCharsets;


public class JSONLoader {
    private ODatabaseSession db;

    public JSONLoader(ODatabaseSession db){
        this.db = db;
    }

    public void loadJSONIntoDB(String fileName, String className) {
        InputStream inputStream = CSVLoader.class.getResourceAsStream("/" + fileName);
        InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        String line;
        StringBuilder responseStrBuilder = new StringBuilder();

        try (BufferedReader br = new BufferedReader(streamReader)) {
            System.out.println("Importing data from " + fileName + "...");
            while ((line = br.readLine()) != null) {
                responseStrBuilder.append(line);
                JSONObject jsonObject = new JSONObject(responseStrBuilder.toString());
                OVertex result = db.newVertex(className);

                for (Object key : jsonObject.keySet()) {
                    String keyStr = (String)key;
                    Object keyvalue = jsonObject.get(keyStr);
                    result.setProperty(keyStr, keyvalue.toString());
                }

                result.save();
            }
            inputStream.close();
            System.out.println("Import successfully completed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
