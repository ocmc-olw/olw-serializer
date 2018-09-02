package delete.me;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.ocmc.olw.serializer.models.GitLabProject;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RestTest2 {
		  
	    
	public static void main(String[] args) {
		try {
			String token = System.getenv("TOKEN");
			String path = URLEncoder.encode("serialized/db2ares/gr_gr_ntpt", "UTF-8");
			
            URL url = new URL("https://gitlab.liml.org/api/v4/projects/" + path);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/xml");
            conn.setRequestProperty("Private-Token", token);
 
            if (conn.getResponseCode() != 200)
            {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }
 
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            String apiOutput = br.readLine();
            System.out.println(apiOutput);
            conn.disconnect();
 
            Gson gson = new Gson();
            JsonParser parser = new JsonParser();
            JsonObject user = (JsonObject) parser.parse(apiOutput);
            GitLabProject p = gson.fromJson(apiOutput, GitLabProject.class);
            p.setPrettyPrint(true);
            System.out.println(user.toString());
            System.out.println(p.toJsonString());
             
        } catch (Exception e) {
        }
    }

}
