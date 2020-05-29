package Loaders;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class CSVLoader {
    private ODatabaseSession db;

    public CSVLoader(ODatabaseSession db){
        this.db = db;
    }

    public void loadCSVIntoDB(String fileName, String separator, String className){
        String line;
        InputStream inputStream = CSVLoader.class.getResourceAsStream("/" + fileName);
        InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        try (BufferedReader br = new BufferedReader(streamReader)) {
            System.out.println("Importing data from " + fileName + "...");
            String[] headers = br.readLine().split(separator);
            while ((line = br.readLine()) != null) {
                String[] data = line.split(separator);

                OVertex result = db.newVertex(className);
                for (int i = 0; i < headers.length; i++) {
                    result.setProperty(headers[i], data[i]);
                }
                result.save();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
