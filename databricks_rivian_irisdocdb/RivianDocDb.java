package databricks_rivian_irisdocdb;

import java.sql.SQLException;
import com.intersystems.document.*;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.File; 
import java.io.FileInputStream;
import org.apache.commons.io.IOUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;



public class RivianDocDb {
  public static void main(String[] args) {


    String directoryPath = "/home/sween/Desktop/POP2/DEEZWATTS/rivian-iris-docdb/databricks_rivian_irisdocdb/in/json/";

    
    DataSource datasrc = DataSource.createDataSource();
    datasrc.setPortNumber(443);
    datasrc.setServerName("k8s-05868f04-a88b7ecb-5c5e41660d-404345a22ba1370c.elb.us-east-1.amazonaws.com");
    datasrc.setDatabaseName("USER");
    datasrc.setUser("SQLAdmin");
    datasrc.setPassword("REDACTED");
    
    try {
      datasrc.setConnectionSecurityLevel(10);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    System.out.println("\nCreated datasrc\n");
    System.out.println(datasrc);
    datasrc.preStart(2);
    System.out.println("\nDataSource size =" + datasrc.getSize());

    // creates the collection if it dont exist
    Collection collectedDocs = Collection.getCollection(datasrc,"deezwatts_mil");
    // Using Files.list() (Java 8 and above)
    try (Stream<Path> paths = Files.list(Paths.get(directoryPath))) {
        paths.filter(Files::isRegularFile)
             .forEach(path -> {
                 File file = path.toFile();
                 //System.out.println(file.getName());
                 // Do something with the file
             });
    } catch (IOException e) {
        e.printStackTrace();
    }

    
    // Using File.listFiles() (Pre-Java 8)
    File directory = new File(directoryPath);
    if (directory.isDirectory()) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    //System.out.println(file.getName());
                    // Do something with the file
                    // create it for now, read in from somewhere for the object for show time
                    // Document doc = new JSONObject().put("Rank",1);
                    
                    try (InputStream is = new FileInputStream("/home/sween/Desktop/POP2/DEEZWATTS/rivian-iris-docdb/databricks_rivian_irisdocdb/in/json/" + file.getName())) {
                      String jsonTxt = IOUtils.toString(is, "UTF-8");
                      //System.out.println(jsonTxt);
                      //JSONObject json = new JSONObject(jsonTxt);
                      //JSONObject jsonObj = new JSONObject(jsonTxt);
                      Document doc = new JSONObject().put("whip",jsonTxt);

                      Document doc2 = JSONObject.fromJSONString(jsonTxt);
                      Document doc3 = new JSONObject().put("whip2",doc2);


                      //System.out.println(doc);
                      //System.out.println(collectedDocs);
                      collectedDocs.insert(doc3);
                    } catch (IOException e) {
                      // TODO Auto-generated catch block
                      e.printStackTrace();
                    }

                }
            }
        }
    }
    


    long size = collectedDocs.size();
    System.out.println(Long.toString(size));
    System.out.println("\nDataSource size =" + datasrc.getSize());
    /*
    // Query Test 
    String queryText = "SELECT TOP 10 * FROM JSON_TABLE(deezwatts2 FORMAT collection)";
    Query query = collectedDocs.createQuery(queryText);
    try {
      Cursor results = query.execute();
      System.out.println("Query returned " + results.count() + " results.");
      while (results.hasNext()) {
          System.out.println(results.next().toJSONString());
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block

      e.printStackTrace();
    }
    //System.out.print(results);
    */



    //System.out.println(Arrays.toString(args));
  }
}
