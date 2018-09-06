package org.ocmc.olw.serializer.ldom;

import java.net.URI;
import java.net.URISyntaxException;

import org.ocmc.ioc.liturgical.schemas.constants.DAYS_OF_PENTECOSTARION;
import org.ocmc.ioc.liturgical.schemas.constants.DAYS_OF_TRIODION;
import org.ocmc.ioc.liturgical.schemas.constants.HEIRMOI_TYPES;
import org.ocmc.ioc.liturgical.schemas.constants.HIERATIKON_SECTIONS;
import org.ocmc.ioc.liturgical.schemas.constants.HOROLOGION_SECTIONS;
import org.ocmc.ioc.liturgical.schemas.constants.LITURGICAL_BOOKS;
import org.ocmc.ioc.liturgical.schemas.constants.LITURGICAL_BOOKS_TO_NEO4J;
import org.ocmc.ioc.liturgical.schemas.constants.MODES;
import org.ocmc.ioc.liturgical.schemas.constants.MONTHS_OF_YEAR_MAP;

public class TitleParts {
	private String unparsedTitle = "";
	private String url = "";
	private boolean isService = true;
	private String name = "";
	private String month = "";
	private String day = "";
	private String mode = "";
	private String displayTitle = "";
	
	public TitleParts(
			String title
			, String url
			) {
		this.unparsedTitle = title;
		this.url = url;
//		if (this.url.endsWith("index.html")) {
			this.parseUrl();
//		} else {
//			this.parseTitle();
// 		}
	}

	private void parseUrl() {
		try {
			String [] titleParts = this.unparsedTitle.split("\\.");
			if (titleParts.length > 1) {
				this.isService = titleParts[0].equals("se");
				this.name = titleParts[titleParts.length-1];
			} else {
				URI uri = new URI(this.url);
				String [] uriParts = null;
				if (uri.getPath().contains("liml.org/static")) {
					uriParts = uri.getPath().split("dcs/h/");
				} else {
					uriParts = uri.getPath().split("dcs/h/");
				}
				uriParts = uriParts[uriParts.length-1].split("/");
				this.isService = uriParts[0].equals("se");
				if (this.isService) {
					this.name = uriParts[uriParts.length-3];
				} else {
					if (uriParts[1].equals("oc")) {
						this.name = uriParts[1];
						this.mode = uriParts[2];
						this.day = uriParts[3];
					} else if (uriParts[1].equals("tr")) {
						this.name = uriParts[1];
						this.day = uriParts[2];
						if (this.day.contains("_")) {
							String []  dayParts = this.day.split("_");
							if (dayParts.length == 2) {
								this.day = dayParts[0];
							}
						}
						this.day = Integer.toString(Integer.parseInt(this.day.substring(1)));
						this.displayTitle = this.name + " Day " + this.day;
					}
				}
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public String getTemplateTitlesId(String library, String keySuffix, boolean ages) {
		StringBuffer sb = new StringBuffer();
		if (ages) {
			sb.append("template.titles_");
			sb.append(library);
			sb.append("|");
			sb.append(this.name);
			sb.append(keySuffix);
		} else {
			sb.append(library);
			sb.append("~template.titles~");
			sb.append(this.name);
			sb.append(keySuffix);
		}
		return sb.toString();
	}
	
	public String getModeId(String library, String keySuffix, boolean ages) {
		StringBuffer sb = new StringBuffer();
		if (ages) {
			sb.append("template.titles_");
			sb.append(library);
			sb.append("|");
			sb.append(this.name);
			sb.append(keySuffix);
		} else {
			sb.append(library);
			sb.append("~template.titles~");
			sb.append(this.name);
			sb.append(keySuffix);
		}
		return sb.toString();
	}

	public String getUnparsedTitle() {
		return unparsedTitle;
	}


	public void setUnparsedTitle(String unparsedTitle) {
		this.unparsedTitle = unparsedTitle;
	}


	public boolean isService() {
		return isService;
	}


	public void setService(boolean isService) {
		this.isService = isService;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getMonth() {
		return month;
	}


	public void setMonth(String month) {
		this.month = month;
	}


	public String getDay() {
		return day;
	}


	public void setDay(String day) {
		this.day = day;
	}


	public String getMode() {
		return mode;
	}


	public void setMode(String mode) {
		this.mode = mode;
	}


	public String getDisplayTitle() {
		return displayTitle;
	}


	public void setDisplayTitle(String displayTitle) {
		this.displayTitle = displayTitle;
	}
	
	public String parseTitle() {
		/**
		 * Cases
		 * Title: OCTOECHOS
		 * URL: https://liml.org/static/oc/h/b/oc.m1.d1.gr-en.html
		 * 
		 * These have title built in:
		 * 		Title: Baptism | Funeral | Memorial | Unction | Wedding
		 * 		Title: se.m01.d06.gw = The Great Blessing of Waters
		 * 		Title: bk.smallwaterblessing
		 */
		StringBuffer sb = new StringBuffer();
		String bookKey = "";
		String [] topicParts = this.unparsedTitle.split("\\.");
		
		if (topicParts.length == 1) {
			String key = LITURGICAL_BOOKS.getKeyForValue(this.unparsedTitle);
			this.name = LITURGICAL_BOOKS_TO_NEO4J.get(key);
		} else {
			this.isService = topicParts[0].startsWith("se");
			
			if (LITURGICAL_BOOKS.containsKey(topicParts[0])) {
				bookKey  = topicParts[0];
				if (bookKey.equals("le")) {
					bookKey = bookKey + "." + topicParts[1];
				}
				if (LITURGICAL_BOOKS.containsKey(bookKey)) {
					sb.append(LITURGICAL_BOOKS.get(bookKey));
					String sectionKey = "";
					String p2 = "";
					if (topicParts.length > 1) {
						sectionKey = topicParts[1];
						switch (bookKey) {
						case ("he"): {
								if (HEIRMOI_TYPES.containsKey(sectionKey)) {
									p2 = HEIRMOI_TYPES.get(sectionKey);
									if (p2.length() > 0) {
										sb.append(", ");
										sb.append(p2);
										if (topicParts.length > 2) {
											sectionKey = topicParts[2];
											String p3 = "";
											if (MODES.containsKey(sectionKey)) {
												p3 = MODES.get(sectionKey);
												if (p3.length() > 0) {
													sb.append(", ");
													sb.append(p3);
												}
											}
										}
									}
								}
								break;
							}
						case ("hi"): {
							if (HIERATIKON_SECTIONS.containsKey(sectionKey)) {
								p2 = HIERATIKON_SECTIONS.get(sectionKey);
								if (p2.length() > 0) {
									sb.append(", ");
									sb.append(p2);
								}
							}
							break;
						}
						case ("ho"): {
							if (HOROLOGION_SECTIONS.containsKey(sectionKey)) {
								p2 = HOROLOGION_SECTIONS.get(sectionKey);
								if (p2.length() > 0) {
									sb.append(", ");
									sb.append(p2);
								}
							}
							break;
						}
						case ("me"): {
							if (MONTHS_OF_YEAR_MAP.containsKey(sectionKey)) {
								p2 = MONTHS_OF_YEAR_MAP.get(sectionKey);
								if (p2.length() > 0) {
									sb.append(", ");
									sb.append(p2);
								}
							}
							break;
						}
						case ("oc"): {
							if (MODES.containsKey(sectionKey)) {
								p2 = MODES.get(sectionKey);
								if (p2.length() > 0) {
									sb.append(", ");
									sb.append(p2);
								}
							}
							break;
						}
						case ("pe"): {
							if (DAYS_OF_PENTECOSTARION.containsKey(sectionKey)) {
								p2 = DAYS_OF_PENTECOSTARION.get(sectionKey);
								if (p2.length() > 0) {
									sb.append(", ");
									sb.append(p2);
								}
							}
							break;
						}
						case ("tr"): {
							if (DAYS_OF_TRIODION.containsKey(sectionKey)) {
								p2 = DAYS_OF_TRIODION.get(sectionKey);
								if (p2.length() > 0) {
									sb.append(", ");
									sb.append(p2);
								}
							}
							break;
						}
						}
					}
				}
		}
		
		}
		return sb.toString();
	}


}

