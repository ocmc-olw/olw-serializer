package org.ocmc.olw.serializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Properties;
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
       		
       		boolean badWhere = false;
       		
    		String whereClause = System.getenv("WHERE_CLAUSE");
    		if (whereClause == null) {
    			whereClause = "";
    		} else {
    			if (whereClause.equals("WHERE") || whereClause.equals("WHERE_NOT")) {
    				// OK
    			} else {
    				badWhere = true;
    			}
    		}

    		String whereLibraries = System.getenv("WHERE_LIBRARIES");
    		if (whereLibraries == null) {
    			if (whereClause.length() > 0) {
    				badWhere = true;
    			}
    			whereLibraries = "";
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

    		int pushDelay       = 1;
    		try {
        		pushDelay = Integer.parseInt(System.getenv("PUSH_DELAY"));
    		} catch (Exception e) {
    			pushDelay = 30000; // 60000 milliseconds = 1 minute
    		}

    		boolean serviceEnabled = true;
    		boolean debugEnabled = false;
    		boolean reinitEnabled = false;

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

    		String propMessagingEnabled = System.getenv("MSG_ENABLED");
    		if (propMessagingEnabled == null) {
    			messagingEnabled = false;
    		} else if (propMessagingEnabled.toLowerCase().equals("true")) {
    			messagingEnabled = true;
    		} else {
    			messagingEnabled = false;
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
	       			logger.info("WHERE_CLAUSE = " + whereClause);
	       			logger.info("WHERE_LIBRARIES = " + whereLibraries);
	       			logger.info("INITIAL_DELAY = " + initialDelay);
	       			logger.info("PERIOD = " + period);
	       			logger.info("TIME_UNIT = " + timeUnit);
	       			logger.info("MSG_ENABLED = " + messagingEnabled);
	       			logger.info("DEBUG_ENABLED = " + debugEnabled);
	       			
	       			if (serviceEnabled) {
	   					executorService.scheduleAtFixedRate(
	   							new Serializer(
	   									db_usr
	   									, db_pwd
	   									, db_url 
	   									, whereClause
	   									, whereLibraries 
	   									, repoDomain
	   									, repoUser
	   									, repoToken
	   									, pushDelay
	   									, debugEnabled
	   									, reinitEnabled
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
