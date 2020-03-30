package org.ocmc.olw.serializer.utils;
 
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.ocmc.ioc.liturgical.utils.FileUtils;

import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

/**
 * Ensures that every tex resource library contains the exact same topic files and keys.
 * @author mac002
 *
 */
public class TexResourceNormalizer {
	
	/**
	 * Row = topic~key
	 * Col = library
	 * Cell = value
	 */
	Map<String,Table<String,String,String>> map = new TreeMap<String,Table<String,String,String>>();
	List<String> libraries = new ArrayList<String>();
	
	String start = "\\itId";
	String terminal = "}%";
	
	public void process(String pathIn, String pathOut) {
		Queue<String> q = new LinkedList<>();
		Table<String, String, String> keyLibraryValueTable = null;
		
		for (File f : FileUtils.getFilesInDirectory(pathIn, "tex")) {
			System.out.println(f.getPath());
			for (String line : FileUtils.linesFromFile(f)) {
				if (line.startsWith(terminal)) {
					String idLine = q.remove().replace("\\itId", "}");
					String value = "";
					if (! q.isEmpty()) {
						value = q.remove();
					}
					String parts[] = idLine.split("\\}\\{");
					if (parts.length == 6) {
						String lang = parts[1];
						String country = parts[2];
						String realm = parts[3];
						String topic = parts[4];
						String key = parts[5];
						String library = lang + "_" + country + "_" + realm;
						if (! this.libraries.contains(library)) {
							this.libraries.add(library);
						}
						if (this.map.containsKey(topic)) {
							keyLibraryValueTable = this.map.get(topic);
						} else {
							keyLibraryValueTable = TreeBasedTable.create();
						}
						keyLibraryValueTable.put(key, library, value);
						this.map.put(topic, keyLibraryValueTable);
					}
				} else {
					q.add(line);
				}
			}
		}
		Collections.sort(libraries);
		// look for libraries without the required topic-key and put in a blank value
		for (String topic : this.map.keySet()) {
			keyLibraryValueTable = this.map.get(topic);
			for (String key : keyLibraryValueTable.rowKeySet()) {
				for (String library : this.libraries) {
					if (! keyLibraryValueTable.contains(key, library)) {
						keyLibraryValueTable.put(key, library, "");
					}
				}
			}
			this.map.put(topic, keyLibraryValueTable);
		}
		// write out the normalized files
		for (String library : this.libraries) {
			for (String topic : this.map.keySet()) {
				StringBuffer sb = new StringBuffer();
				for (String key : this.map.get(topic).rowKeySet()) {
					sb.append("\\itId{");
					sb.append(library);
					sb.append("}{");
					sb.append(topic);
					sb.append("}{");
					sb.append(key);
					sb.append("}{\n");
					String value = this.map.get(topic).get(key, library);
					if (value.length() > 0) {
						sb.append(value);
						sb.append("\n");
					}
					sb.append("}%\n");
				}
				FileUtils.writeFile(pathOut + library + "/res." + topic + ".tex", sb.toString());
			}
		}
	}

	public static void main(String[] args) {
		String pathIn = "/private/var/lib/gitlab/serialized/db2tex";
		String pathOut = "/private/var/lib/gitlab/serialized/resources/";
		TexResourceNormalizer n = new TexResourceNormalizer();
		n.process(pathIn, pathOut);
	}

}
