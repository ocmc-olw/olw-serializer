package org.ocmc.olw.serializer.utils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

import com.google.gson.JsonObject;

import net.ages.alwb.utils.core.datastores.neo4j.Neo4jConnectionManager;

/**
 * Run this to compare the nodes (n:Root) between two Neo4j instances.
 * It will report nodes that are missing (i.e.)
 * in the first db, not in second
 * in the second db, not in first
 * 
 * The comparison is based on the id of each node (library~topic~key).
 * 
 * @author mac002
 *
 */
public class CompareDbInstances {
	private String uid = "";
	private String pwd = "";
	private String url1 = "";
	private String url2 = "";
	
	private List<String> db1ids = new ArrayList<String>();
	private List<String> db2ids = new ArrayList<String>();

	private Neo4jConnectionManager db = null;

	public CompareDbInstances(
			String uid
			, String pwd
			, String url1
			, String url2
			) {
		this.uid = uid;
		this.pwd = pwd;
		this.url1 = url1;
		this.url2 = url2;
	}
	
	public void process() {
		this.loadList2(this.url1, this.db1ids);
		this.loadList2(this.url2, this.db2ids);
//		this.compare2(this.url1, this.url2, this.db1ids, this.db2ids);
//		this.compare2(this.url2, this.url1, this.db2ids, this.db1ids);
		this.compare3(this.url1, this.url2, this.db1ids, this.db2ids);
	}
	

	private void compare3(
			String url1
			, String url2
			, List<String> l1
			, List<String> l2
			) {
		System.out.println("Finding ids in " + url1 + " not in " + url2 + Instant.now().toString());
//		Collections.sort(l1);
//		Collections.sort(l2);
		
		int size1 = l1.size();
		int size2 = l2.size();
		int i1 = 0;
		int i2 = 0;
		int l1Missing = 0;
		int l2Missing = 0;
		
		while (i1 < size1 || i2 < size2) {
		    String s1 = "";
			String s2 = "";
			if (i1 < size1) {
			    s1 = l1.get(i1);
			}
			if (i2 < size2) {
				s2 = l2.get(i2);
			}
//			System.out.println(s1 + " ? " + s2);
			if (s1.length() == 0) {
				System.out.println(url1 + " missing " + s2);
				l1Missing++;
				i2++;
			} else if (s2.length() == 0) {
				System.out.println(url2 + " missing " + s1);
				l2Missing++;
				i1++;
			} else {
				int compResult = s1.compareTo(s2);
				if (compResult < 0) {
					System.out.println(url2 + " missing " + s1);
					l2Missing++;
					i1++;
				} else if (compResult == 0) {
					i1++;
					i2++;
				} else {
					System.out.println(url1 + " missing " + s2);
					l1Missing++;
					i2++;
				}
			}
		}
		System.out.println("Finished finding ids in " + url1 + " not in " + url2 + Instant.now().toString());
		System.out.println(url1 + " missing " + l1Missing);
		System.out.println(url2 + " missing " + l2Missing);
	}
	
	private synchronized void loadList2(String url, List<String> list) {
		db = new Neo4jConnectionManager(
				  url
				  , this.uid
				  , this.pwd
				  , false
				  );

		System.out.println(url + " started creating list: " + Instant.now().toString());
		String queryLibs = "match (n:Root) return n.id as id order by n.id;";
		ResultJsonObjectArray libsResult = db.getForQuery(queryLibs);
		for (JsonObject iobj : libsResult.getValues()) {
			try {
				String id = iobj.get("id").getAsString();
				list.add(id);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println(url + " finished creating list " + Instant.now().toString());
	}


	public static void main(String[] args) {
		String uid = System.getenv("UID");
		String pwd = System.getenv("PWD");
		String url1 = System.getenv("URL_1");
		String url2 = System.getenv("URL_2");
		CompareDbInstances it = new CompareDbInstances(uid, pwd,url1,url2);
		it.process();
	}

}
