package net.ages.alwb.utils.core.datastores.neo4j;

import java.util.List;

import org.ocmc.ioc.liturgical.schemas.models.db.internal.LTKVJsonObject;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.RequestStatus;

import com.google.gson.JsonObject;

public interface LowLevelDataStoreInterface {

	/**
     * Inserts a single JsonObject
     * @param doc - to be inserted
     * @throws DbException an exception that occurs in the database
     */
    public void insert(JsonObject doc) throws DbException; 

    /**
     * Inserts all JsonObjects in the list
     * @param docs the docs
     * @throws DbException an exception in the database
     */
    public void insert(List<JsonObject> docs) throws DbException;
    
    /**
     * Updates a single JsonObject (i.e., one that exists).
     * The _id for the update is retrieved from the JsonObject.
     * @param doc - to be inserted 
     * @throws DbException an exception in the database
     * @return the RequestStatus
     */
    public RequestStatus updateWhereEqual(LTKVJsonObject doc) throws DbException;

	public RequestStatus insert(LTKVJsonObject doc) throws DbException;
    
    public RequestStatus deleteNodeWhereEqual(String id) throws DbException;
	
}
