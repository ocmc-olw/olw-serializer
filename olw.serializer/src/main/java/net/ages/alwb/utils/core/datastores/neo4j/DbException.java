package net.ages.alwb.utils.core.datastores.neo4j;

/***
 * Acts as a wrapper to an underlying database exception, e.g. SQL Exception
 * @author mac002
 *
 */
public class DbException extends Exception {
	
	public DbException(String message) {
        super(message);
    }

    public DbException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
