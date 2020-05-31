package Loader;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

public class DataLoader {
    private ODatabaseSession db;

    public DataLoader(ODatabaseSession db) {
        this.db = db;
    }

    public void loadCSVIntoDB(String fileName, String separator, String className) {
        ZipLoader zL = new ZipLoader("data.zip");
        String line;

        try (InputStream inputStream = zL.getInputStreamFromZip(fileName);
             InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             BufferedReader br = new BufferedReader(streamReader)) {
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
            System.out.println("Import successfully completed");
            zL.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadJSONIntoDB(String fileName, String className) {
        ZipLoader zL = new ZipLoader("data.zip");
        String line;

        try (InputStream inputStream = zL.getInputStreamFromZip(fileName);
             InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             BufferedReader br = new BufferedReader(streamReader)) {
            System.out.println("Importing data from " + fileName + "...");
            while ((line = br.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                OVertex result = db.newVertex(className);
                for (Object key : jsonObject.keySet()) {
                    String keyStr = (String) key;
                    if (jsonObject.get(keyStr) instanceof JSONArray) {
                        result.setProperty(keyStr, jsonObject.getJSONArray(keyStr).toList());
                    } else {
                        result.setProperty(keyStr, jsonObject.get(keyStr));
                    }
                }
                result.save();
            }
            System.out.println("Import successfully completed");
            zL.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadCSVEdgeIntoDB(String fileName, String separator, String className) {
        ZipLoader zL = new ZipLoader("data.zip");
        String line;
        int tagCount = 0;

        try (InputStream inputStream = zL.getInputStreamFromZip(fileName);
             InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             BufferedReader br = new BufferedReader(streamReader)) {
            System.out.println("generating data for edge class " + className + " using file " + fileName + "...");
            String[] headers = br.readLine().split(separator);
            while ((line = br.readLine()) != null) {
                String[] data = line.split(separator);

                // We query the in and out vertexes for our edge
                String[] firstHeaderData = headers[0].split("\\.");
                String firstQuery = "SELECT from " + firstHeaderData[0] + " where " + firstHeaderData[1] + " = ?";
                OResultSet firstRs = db.query(firstQuery, data[0]);

                String[] secondHeaderData = headers[1].split("\\.");
                String secondQuery = "SELECT from " + secondHeaderData[0] + " where " + secondHeaderData[1] + " = ?";
                OResultSet secondRs = db.query(secondQuery, data[1]);

                // Here we have to handle the 'Tag' case, since we don't have any data for tags, we want to generate one if it is missing.
                if (firstRs.hasNext() && (secondRs.hasNext() || secondHeaderData[0].equals("Tag"))) {
                    // Create a temporary vertex for a possibly missing tag
                    OVertex secondVertex = db.newVertex("Tag");
                    secondVertex.setProperty("id", data[1]);
                    secondVertex.setProperty("label", "This is the tag number " + tagCount);

                    OResult firstRes = firstRs.next();

                    // If the query has returned a result, we override the temporary vertex with it
                    if (secondRs.hasNext()) {
                        OResult secondRes = secondRs.next();
                        Optional<OVertex> optionalSecondVertex = secondRes.getVertex();

                        if (optionalSecondVertex.isPresent()) {
                            secondVertex = optionalSecondVertex.get();
                        }
                    } else {
                        // Otherwise, we create it and increase the tagCount variable.
                        secondVertex.save();
                        tagCount++;
                    }

                    Optional<OVertex> optionalFirstVertex = firstRes.getVertex();

                    if (optionalFirstVertex.isPresent()) {
                        OVertex firstVertex = optionalFirstVertex.get();
                        OEdge edge = firstVertex.addEdge(secondVertex, className);
                        for (int i = 2; i < headers.length; i++) {
                            edge.setProperty(headers[i], data[i]);
                        }
                        firstRs.close();
                        secondRs.close();
                        edge.save();
                    }
                }
            }
            System.out.println("Edge successfully generated");
            zL.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadXMLIntoDB(String fileName, String className, ArrayList<String> arrayNodeNames) {
        ZipLoader zL = new ZipLoader("data.zip");
        try (InputStream inputStream = zL.getInputStreamFromZip(fileName)) {
            if (inputStream == null) {
                System.out.println("File not found");
            } else {
                DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                Document doc = dBuilder.parse(inputStream);
                System.out.println("Importing data from " + fileName + "...");
                doc.getDocumentElement().normalize();

                if (doc.hasChildNodes()) {
                    loadNodesIntoDB(doc.getDocumentElement().getChildNodes(), className, arrayNodeNames);
                }
                System.out.println("Import successfully completed");
            }
            zL.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadNodesIntoDB(NodeList nodeList, String className, ArrayList<String> arrayNodeNames) {
        for (int count = 0; count < nodeList.getLength(); count++) {
            Node tempNode = nodeList.item(count);
            if (tempNode.getNodeType() == Node.ELEMENT_NODE) {
                if (tempNode.hasChildNodes()) {
                    OVertex result = db.newVertex(className);
                    HashMap<String, ArrayList<HashMap<String, String>>> mapOfEmbeddedLists = new HashMap<>();
                    for (int i = 0; i < tempNode.getChildNodes().getLength(); i++) {
                        Node n = tempNode.getChildNodes().item(i);
                        if (n.getNodeType() == Node.ELEMENT_NODE) {
                            String nodeName = n.getNodeName();
                            if (arrayNodeNames.contains(nodeName)) {
                                if (mapOfEmbeddedLists.containsKey(nodeName)) {
                                    mapOfEmbeddedLists.get(nodeName).add(buildHashMapFromNode(n));
                                } else {
                                    ArrayList<HashMap<String, String>> aL = new ArrayList<>();
                                    aL.add(buildHashMapFromNode(n));
                                    mapOfEmbeddedLists.put(nodeName, aL);
                                }
                            } else {
                                result.setProperty(nodeName, n.getTextContent());
                            }
                        }
                        for (HashMap.Entry key : mapOfEmbeddedLists.entrySet()) {
                            result.setProperty(key.getKey().toString(), key.getValue());
                        }
                    }
                    result.save();
                }
            }
        }
    }

    private HashMap<String, String> buildHashMapFromNode(Node n) {
        HashMap<String, String> hm = new HashMap<>();
        if (n.hasChildNodes()) {
            for (int i = 0; i < n.getChildNodes().getLength(); i++) {
                Node tempNode = n.getChildNodes().item(i);
                if (tempNode.getNodeType() == Node.ELEMENT_NODE) {
                    hm.put(tempNode.getNodeName(), tempNode.getTextContent());
                }
            }
        }
        return hm;
    }
}
