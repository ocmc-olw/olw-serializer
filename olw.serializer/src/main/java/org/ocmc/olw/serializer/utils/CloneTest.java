package org.ocmc.olw.serializer.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.ocmc.ioc.liturgical.schemas.constants.SCHEMA_CLASSES;
import org.ocmc.ioc.liturgical.schemas.constants.VISIBILITY;
import org.ocmc.ioc.liturgical.schemas.id.managers.IdManager;
import org.ocmc.ioc.liturgical.schemas.models.supers.LTKDb;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.ages.alwb.utils.core.datastores.neo4j.Neo4jConnectionManager;

public class CloneTest {

	private String uid = "";
	private String pwd = "";
	private String url = "";
	
	private Neo4jConnectionManager db = null;
	private  Gson gson = new Gson();


	public CloneTest(
			String uid
			, String pwd
			, String url
			) {
		this.uid = uid;
		this.pwd = pwd;
		this.url = url;
		db = new Neo4jConnectionManager(
				  this.url
				  , this.uid
				  , this.pwd
				  , false
				  );
	}
	
	public List<String> getTopicKeys(String library) {
		List<String> result = new ArrayList<String>();
		try {
			String query = "match (n:Root) where n.library = '" + library + "' and size(trim(n.value)) > 0 return n.topic + '~' + n.key as topicKey order by topicKey;";
			ResultJsonObjectArray qResult = db.getForQuery(query);
			for (JsonObject o : qResult.getValues()) {
				try {
					String topicKey = o.get("topicKey").getAsString();
					result.add(topicKey);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private List<String> getDiff(String lib1, String lib2) {
		List<String> result = new ArrayList<String>();
		try {
			Collection<String> lib1TopicKeys = this.getTopicKeys(lib1);
			Collection<String> lib2TopicKeys = this.getTopicKeys(lib2);
			result = new ArrayList<String>(lib1TopicKeys);
			result.removeAll(lib2TopicKeys);
		} catch (Exception e) {
			e.printStackTrace();;
		}
		
		return result;
	}
	
	public void cloneLibrary (String libFrom, String libTo) {
		try {
			for (String topicKey : this.getDiff(libFrom, libTo)) {
				String query = "match (n:Root) where n.id = '" + libFrom + "~" + topicKey + "' return properties(n) as props";
				ResultJsonObjectArray qResult = db.getForQuery(query);
				JsonObject o = qResult.getFirstObject().get("props").getAsJsonObject();
				LTKDb doc = 
						gson.fromJson(
								o.toString()
								, SCHEMA_CLASSES
									.classForSchemaName(
											o.get("_valueSchemaId").getAsString())
									.ltkDb.getClass()
					);
				doc.setLibrary(libTo);
				IdManager idManager = new IdManager(libTo, o.get("topic").getAsString(), o.get("key").getAsString());
				doc.setId(idManager.getId());
				doc.setVisibility(VISIBILITY.PRIVATE);
				System.out.println(doc.getId());
				db.insert(doc);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args) {
		String uid = System.getenv("UID");
		String pwd = System.getenv("PWD");
		String url = System.getenv("URL");
		String libFrom = System.getenv("LIB_FROM");
		String libTo = System.getenv("LIB_TO");

		CloneTest it = new CloneTest(uid, pwd,url);
		it.cloneLibrary(libFrom,libTo);
	}

}
