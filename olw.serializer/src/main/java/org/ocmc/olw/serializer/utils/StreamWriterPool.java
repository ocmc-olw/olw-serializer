package org.ocmc.olw.serializer.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

/**
 * Holds a map of PrintWriters, one per schema
 * that are streams that can be written to.
 * They are flushed when the close writers method is called.
 * 
 * @author mac002
 *
 */
public class StreamWriterPool {
	private String pathOut = "";
	private Map<String,PrintWriter> streamMap = new TreeMap<String,PrintWriter>();
	private List<String> paths = new ArrayList<String>();
	
	public StreamWriterPool(String pathOut) {
		this.pathOut = pathOut;
		if (! this.pathOut.endsWith("/")) {
			this.pathOut = pathOut + "/";
		}
		File dir = new File(this.pathOut);
		if (dir.exists()) {
			try {
				FileUtils.cleanDirectory(dir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			dir.mkdirs();
		}
	}
	
	public void openWriter(String filename) {
		try {
			File f = new File(this.pathOut + filename + ".csv");
			if (! f.exists()) {
				f.getParentFile().mkdirs();
				f.createNewFile();
			}
			this.paths.add(f.getAbsolutePath());
			FileWriter fw = new FileWriter(f,true);
	    	  BufferedWriter bw = new BufferedWriter(fw);
	    	  PrintWriter pw = new PrintWriter(bw);
			this.streamMap.put(filename, pw);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void write(String filename, String content) {
		PrintWriter pw = null;
		if (! this.streamMap.containsKey(filename)) {
			this.openWriter(filename);
		}
		pw = this.streamMap.get(filename);
		if (content.endsWith("\n")) {
			pw.print(content);
		} else {
			pw.println(content);
		}
		this.streamMap.put(filename, pw);
	}
	
	public void closeWriters() {
		for (PrintWriter pw : this.streamMap.values()) {
			pw.flush();
			pw.close();
		}
	}

	public List<String> getPaths() {
		return paths;
	}

	public void setPaths(List<String> paths) {
		this.paths = paths;
	}
	
}
