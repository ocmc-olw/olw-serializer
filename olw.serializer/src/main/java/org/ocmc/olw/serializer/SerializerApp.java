package org.ocmc.olw.serializer;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.ocmc.ioc.liturgical.utils.ErrorUtils;
import org.ocmc.ioc.liturgical.utils.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point to the serialization process.
 * Serializes Neo4j nodes and relationships to json
 * and commits them to gitlab.liml.org.
 *
 *11-20-2019 Serializer.java is for Gitlab.  SerializerGithub is for Github.
 * 04-01-2020 Using SerializerGithub, but saveit shell script pushes to Gitlab.
 */
public class SerializerApp {
	private static final Logger logger = LoggerFactory.getLogger(SerializerApp.class);

	private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	private static String messagingToken = "";
	private static boolean messagingEnabled = false;
	
	/**
	 * If the property is null, the method returns
	 * back the value of var, otherwise it checks
	 * prop to see if it starts with "true".  If so
	 * it returns true, else false.  
	 * 
	 * The var is passed in so that if the config file lacks 
	 * the specified property, the default value gets used.
	 * @param var the variable
	 * @param prop the property
	 * @return true if so
	 */
	public static boolean toBoolean(boolean var, String prop) {
		if (prop == null) {
			return var;
		} else {
			return prop.startsWith("true");
		}
	}

	/**
	 * The main method must be provided the database admin username and password
	 * and a valid token for Github and one for Slack.  These can be passed via 
	 * command line arguments, or if using docker-compose, they are passed 
	 * through the setting of environment key-values.
	 * @param args - unused
	 */
	public static void main( String[] args ) {
    	try {
    		/**
    		 * Read in the environmental variables.  
    		 */
    		String db_usr =        System.getenv("DB_UID");
    		String db_pwd =      System.getenv("DB_PWD");
    		String db_url =         System.getenv("DB_URL");
       		
    		String gitlabGroup =         System.getenv("GITLAB_GROUP");
  
    		// these next 3 variables are used if running at a fixed time each day
    		Calendar calendar = Calendar.getInstance();
    		long startScheduler = 0;
    		long stopScheduler = 0;

    		boolean badWhere = false;
       		
    		String whereLibrariesClause = System.getenv("WHERE_LIBRARIES_CLAUSE");
    		if (whereLibrariesClause == null) {
    			whereLibrariesClause = "";
    		} else {
    			if (whereLibrariesClause.equals("WHERE") || whereLibrariesClause.equals("WHERE_NOT")) {
    				// OK
    			} else {
    				badWhere = true;
    			}
    		}

    		String whereLibraries = System.getenv("WHERE_LIBRARIES");
    		if (whereLibraries == null) {
    			if (whereLibrariesClause.length() > 0) {
    				badWhere = true;
    			}
    			whereLibraries = "";
    		}

    		String whereSchemasClause = System.getenv("WHERE_SCHEMAS_CLAUSE");
    		if (whereSchemasClause == null) {
    			whereSchemasClause = "";
    		} else {
    			if (whereSchemasClause.equals("WHERE") || whereSchemasClause.equals("WHERE_NOT")) {
    				// OK
    			} else {
    				badWhere = true;
    			}
    		}

    		String whereSchemas = System.getenv("WHERE_SCHEMAS");
    		if (whereSchemas == null) {
    			if (whereSchemasClause.length() > 0) {
    				badWhere = true;
    			}
    			whereSchemas = "";
    		}

    		String texLibraries = System.getenv("TEX_LIBRARIES");
    		if (texLibraries == null) {
    			texLibraries = "";
    		}

    		String texRealms = System.getenv("TEX_REALMS");
    		if (texRealms == null) {
    			texRealms = "";
    		}

    		int initialDelay       = 1;
    		int period               = 4;
    		try {
        		initialDelay = Integer.parseInt(System.getenv("INITIAL_DELAY"));
    		} catch (Exception e) {
    			initialDelay = 1;
    		}
    		try {
        		period = Integer.parseInt(System.getenv("PERIOD"));
    		} catch (Exception e) {
    			period = 4;
    		}

    		String unit = System.getenv("TIME_UNIT");
    		TimeUnit timeUnit = TimeUnit.HOURS;
    		if (unit != null) {
        		switch (unit) {
        		case ("AM"): {
        			timeUnit = TimeUnit.MILLISECONDS;
        			break;
        		}
        		case ("PM"): {
        			timeUnit = TimeUnit.MILLISECONDS;
        			break;
        		}
        		case ("SECONDS"): {
        			timeUnit = TimeUnit.SECONDS;
        			break;
        		}
        		case ("MINUTES"): {
        			timeUnit = TimeUnit.MINUTES;
        			break;
        		}
        		default: {
        			timeUnit = TimeUnit.HOURS;
        		}
        		}
    		}

    		// if timeUnit is milliseconds we will run at a specific time each day.
    		// In this case, we interpret period to mean the hour of the day.
    		// The unit must be set to either AM or PM
    		if (timeUnit == TimeUnit.MILLISECONDS) {
				calendar.set(Calendar.HOUR, period);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				if (unit.equals("AM")) {
					calendar.set(Calendar.AM_PM, Calendar.AM);
				} else {
					calendar.set(Calendar.AM_PM, Calendar.PM);
				}
				Long currentTime = new Date().getTime();
				// if we are already past the time, set to start tomorrow
				if (calendar.getTime().getTime() < currentTime) {
					calendar.add(Calendar.DATE, 1);
				}
				try {
					initialDelay = (int) (calendar.getTime().getTime() - currentTime);
					// allow the task 4 hours to run.  Normally takes just 1.
					calendar.add(Calendar.HOUR, 4);
					period = (int) (calendar.getTime().getTime() - currentTime);
				} catch (Exception e) {
					System.out.println(e.getStackTrace());
				}
    		}
    		int pushDelay       = 1;
    		try {
        		pushDelay = Integer.parseInt(System.getenv("PUSH_DELAY"));
    		} catch (Exception e) {
    			pushDelay = 30000; // 60000 milliseconds = 1 minute
    		}

    		boolean serviceEnabled = true;
    		boolean debugEnabled = false;
    		boolean reinitEnabled = false;
    		boolean pushEnabled = true;
    		boolean db2aresEnabled = true;
    		boolean db2jsonEnabled = true;
    		boolean db2csvEnabled = true;
    		boolean db2texEnabled = true;
    		boolean v4 = false;
  
    		String propServiceEnabled = System.getenv("SERVICE_ENABLED");
    		if (propServiceEnabled != null && propServiceEnabled.toLowerCase().equals("false")) {
    			serviceEnabled = false;
    		}

    		String propDebugEnabled = System.getenv("DEBUG_ENABLED");
    		if (propDebugEnabled != null && propDebugEnabled.toLowerCase().equals("true")) {
    			debugEnabled = true;
    		}

    		String propReinitEnabled = System.getenv("REINIT");
    		if (propReinitEnabled != null && propReinitEnabled.toLowerCase().equals("true")) {
    			reinitEnabled = true;
    		}

    		String propPushEnabled = System.getenv("PUSH_ENABLED");
    		if (propPushEnabled != null && propPushEnabled.toLowerCase().equals("false")) {
    			pushEnabled = false;
    		}

    		String propMessagingEnabled = System.getenv("MSG_ENABLED");
    		if (propMessagingEnabled == null) {
    			messagingEnabled = false;
    		} else if (propMessagingEnabled.toLowerCase().equals("true")) {
    			messagingEnabled = true;
    		} else {
    			messagingEnabled = false;
    		}

       		String propDb2AresEnabled = System.getenv("DB2ARES_ENABLED");
    		if (propDb2AresEnabled != null && propDb2AresEnabled.toLowerCase().equals("false")) {
    			db2aresEnabled = false;
    		}

       		String propDb2JsonEnabled = System.getenv("DB2JSON_ENABLED");
    		if (propDb2JsonEnabled != null && propDb2JsonEnabled.toLowerCase().equals("false")) {
    			db2jsonEnabled = false;
    		}

       		String propDb2CsvEnabled = System.getenv("DB2CSV_ENABLED");
    		if (propDb2CsvEnabled != null && propDb2CsvEnabled.toLowerCase().equals("false")) {
    			db2csvEnabled = false;
    		}

       		String propV4 = System.getenv("V4");
    		if (propV4 != null && propV4.toLowerCase().equals("true")) {
    			v4 = true;
    		}
       		String propDb2TexEnabled = System.getenv("DB2TEX_ENABLED");
    		if (propDb2TexEnabled != null && propDb2TexEnabled.toLowerCase().equals("false")) {
    			db2texEnabled = false;
    		}

    		String strMessagingToken = System.getenv("MSG_TOKEN");
    		if (strMessagingToken == null) {
    			messagingEnabled = false;
    		} else {
    			messagingToken = strMessagingToken;
    		}

    		String repoToken = System.getenv("REPO_TOKEN");
    		String repoUser = System.getenv("REPO_USER");
    		String repoDomain = System.getenv("REPO_DOMAIN");
    		
    		try {
           		if (db_usr == null || db_pwd == null || db_url == null 
           				||  repoToken == null
           				||  repoUser == null
           				||  repoDomain == null
           				) {
          			logger.error("DB_USR, DB_PWD, DB_URL, REPO_TOKEN, REPO_USER, REPO_DOMAIN  are required. Stopping the app.");
           		} else if (badWhere) {
           			logger.error("WHERE_CLAUSE must = either WHERE or WHERE_NOT. If WHERE_CLAUSE used must provide WHERE_LIBRARIES");
           		} else {
         			 logger.info("Neo4j to Json Serializer version: " + Constants.VERSION);
	       			logger.info("logger info enabled = " + logger.isInfoEnabled());
	       			logger.info("logger warn enabled = " + logger.isWarnEnabled());
	       			logger.info("logger trace enabled = " + logger.isTraceEnabled());
	       			logger.info("logger debug enabled = " + logger.isDebugEnabled());
	       			logger.debug("If you see this, logger.debug is working");
	       			SerializerApp.class.getClassLoader();
	       			String location = getLocation();
	       			logger.info("Jar is executing from: " + location);
	       			logger.info("SERVICE_ENABLED = " + serviceEnabled);
	       			logger.info("DB_URL = " + db_url);
	       			logger.info("GIT_PATH = " + Constants.GIT_FOLDER);
	       			logger.info("WHERE_CLAUSE = " + whereLibrariesClause);
	       			logger.info("WHERE_LIBRARIES = " + whereLibraries);
	       			logger.info("INITIAL_DELAY = " + initialDelay);
	       			logger.info("PERIOD = " + period);
	       			logger.info("TIME_UNIT = " + timeUnit);
	       			logger.info("MSG_ENABLED = " + messagingEnabled);
	       			logger.info("DEBUG_ENABLED = " + debugEnabled);
	       			logger.info("REINIT_ENABLED = " + reinitEnabled);
	       			logger.info("PUSH_ENABLED = " + pushEnabled);
	       			
	       			if (serviceEnabled) {
	   					executorService.scheduleAtFixedRate(
	   							new SerializerGithub(
	   									db_usr
	   									, db_pwd
	   									, db_url 
	   									, gitlabGroup
	   									, whereLibrariesClause
	   									, whereLibraries 
	   									, whereSchemasClause
	   									, whereSchemas 
	   									, repoDomain
	   									, repoUser
	   									, repoToken
	   									, texLibraries
	   									, texRealms
	   									, pushDelay
	   									, db2aresEnabled
	   									, db2jsonEnabled
	   									, db2csvEnabled
	   									, db2texEnabled
	   									, debugEnabled
	   									, reinitEnabled
	   									, pushEnabled
	   									, v4
	   									)
	   							, initialDelay
	   							, period
	   							, timeUnit
	   							);
	
	       			}
	           		}
	
	    		} catch (Exception e) {
	    			ErrorUtils.report(logger, e);
	    		}
    	} catch (ArrayIndexOutOfBoundsException arrayError) {
    		logger.error("You failed to pass in one or more of: username, password, github token, slack token");
    	} catch (Exception e) {
    		ErrorUtils.report(logger, e);
    	}
    }
	
	  public static String getLocation() {
		  try {
			return new File(SerializerApp.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getParent();
		} catch (URISyntaxException e) {
			ErrorUtils.report(logger, e);
			return null;
		}
	  }
	  
	  public static String sendMessage(String message) {
		  if (messagingEnabled) {
			  String response = "";
			  MessageUtils.sendMessage(messagingToken, message);
			  return response;
		  } else {
			  return "Messaging not enabled";
		  }
	  }

}
