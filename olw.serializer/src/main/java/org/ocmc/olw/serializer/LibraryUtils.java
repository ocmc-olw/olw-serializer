package org.ocmc.olw.serializer;

import java.io.File;
import java.util.Iterator;

public class LibraryUtils {
	
	public final static String QUOTE = "\"";


	/**
	 * Remove leading and trailing quote from a quoted string
	 * @param s the string
	 * @return the string
	 */
	public static String trimQuotes(String s) {
		String result = s;
		if (s.length() > 2) {
			if (s.startsWith(QUOTE) || s.startsWith("“")) {
				result = s.substring(1, s.length());
			}
			if (s.endsWith(QUOTE) || s.endsWith("”")) {
				result = result.substring(0, s.length()-2);
			}
		} else if (s.compareTo(QUOTE+QUOTE) == 0) {
			result = "";
		}
		return result;
	}
	
	/**
	 * Wrap the string in quotes
	 * @param s the string
	 * @return the string
	 */
	public static String wrapQuotes(String s) {
		if (s.length() > 0) {
			return QUOTE + escapeQuotes(s) + QUOTE;
		} else {
			return QUOTE+QUOTE;
		}
	}
	
	
	public static String escapeQuotes(String text) {
		return text.replaceAll(QUOTE, "\\\\"+QUOTE);
	}
	
	public static String unescapeQuotes(String text) {
			return text.replaceAll("\\\\"+QUOTE,QUOTE);
	}

}
