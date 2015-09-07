package com.rw.Recommender;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.bson.types.ObjectId;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.mongoStore;

public class LeadRecommenderModel implements DataModel {

	static final Logger log = Logger.getLogger(LeadRecommenderModel.class.getName());
	private static mongoStore db = new mongoStore();
	private DataModel delegate;

	private final ReentrantLock reloadLock;
	public FastByIDMap<Collection<Preference>> preferenceMap = new FastByIDMap<Collection<Preference>>();
		  
	public LeadRecommenderModel () {
		this.reloadLock = new ReentrantLock();
	}
	
	@Override
	public String toString() {
	    return preferenceMap.toString();
	}
	
	public void buildModel() {
	    delegate = new GenericDataModel(GenericDataModel.toDataMap(preferenceMap, true));
	}

	public void refresh(Collection<Refreshable> arg0) {
		// TODO Auto-generated method stub
	}

	public LongPrimitiveIterator getItemIDs() throws TasteException {
	    return delegate.getItemIDs();
	}

	public FastIDSet getItemIDsFromUser(long arg0) throws TasteException {
		return delegate.getItemIDsFromUser(arg0);
	}

	public float getMaxPreference() {
	    return delegate.getMaxPreference();
	}

	public float getMinPreference() {
		   return delegate.getMinPreference();
	}

	public int getNumItems() throws TasteException {
	    return delegate.getNumItems();
	}

	public int getNumUsers() throws TasteException {
	    return delegate.getNumUsers();
	}

	public int getNumUsersWithPreferenceFor(long arg0) throws TasteException {
	      return delegate.getNumUsersWithPreferenceFor(arg0);
	}

	public int getNumUsersWithPreferenceFor(long arg0, long arg1)
			throws TasteException {
	      return delegate.getNumUsersWithPreferenceFor(arg0, arg1);
	}

	public Long getPreferenceTime(long partyId, long actionId) throws TasteException {
	    return delegate.getPreferenceTime(partyId, actionId);
	}

	public Float getPreferenceValue(long partyid, long actionId) throws TasteException {
	    return delegate.getPreferenceValue(partyid, actionId);
	}

	public PreferenceArray getPreferencesForItem(long arg0)
			throws TasteException {
	    return delegate.getPreferencesForItem(arg0);
	}

	public PreferenceArray getPreferencesFromUser(long id)
			throws TasteException {
		return delegate.getPreferencesFromUser(id);
	}

	public LongPrimitiveIterator getUserIDs() throws TasteException {
	    return delegate.getUserIDs();
	}

	public boolean hasPreferenceValues() {
	    return delegate.hasPreferenceValues();
	}

	public void removePreference(long partyId, long actionId) throws TasteException {
    	 preferenceMap.remove(partyId);
	}

	public void setPreference(long partyId, long actionId, float preference)
			throws TasteException {
    	Collection<Preference> preferences = preferenceMap.get(partyId);
 	    if (preferences == null) {
	        	preferences = Lists.newArrayListWithCapacity(2);
	        	preferenceMap.put(partyId, preferences);
	    }
	    preferences.add(new GenericPreference(partyId, actionId, preference));
	}

	public int AddCriteria(Long l_user_id, String filter, String partyId) throws Exception {
		RWJApplication app = new RWJApplication();
		BasicDBObject query = new BasicDBObject();
		
		query.clear();
		query.put("partyId", partyId);
		query.put("partyRelation", filter);

		RWJBusComp bcCriteria = app.GetBusObject("Criteria").getBusComp("Criteria");

		int nRecs = bcCriteria.ExecQuery(query);
		
		for ( int i = 0 ; i < nRecs; i++) {

			Long l_item_id = (Long) bcCriteria.GetFieldValue("MLID");
	    	float f_score = 0.5f;
			try {
				setPreference(l_user_id,l_item_id,f_score);
			} catch (TasteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    log.info("AddCriteria: " + String.valueOf(l_user_id) + ":" + String.valueOf(l_item_id) + ":" + String.valueOf(f_score));    
			bcCriteria.NextRecord();
		}
		return nRecs;
	}
	
	public void SetUpDescriptors(String credId, String partyId) throws Exception {

		RWJApplication app = new RWJApplication();
		BasicDBObject query = new BasicDBObject();
		
		RWJBusComp bcParty = app.GetBusObject("Party").getBusComp("Party");
		
		query.put("credId",credId);

		int nRecs = bcParty.ExecQuery(query);
		
		RWJBusComp bcPartner = app.GetBusObject("Partner").getBusComp("Partner");
		RWJBusComp bcCriteria = app.GetBusObject("Criteria").getBusComp("Criteria");
		query.clear();
		query.put("userId", credId);

		int noPartners = bcPartner.ExecQuery(query);
		for ( int i = 0 ; i < noPartners; i++) {
			// Get one the partners descriptors
			String partyId2 = bcPartner.GetFieldValue("partnerId").toString();
			if ( partyId.equals(partyId2)) continue;
			String partnerId2 = bcPartner.GetFieldValue("_id").toString();
			Long mlid = (Long) bcPartner.GetFieldValue("MLID");
			AddCriteria(mlid, "DESCRIPTOR", partyId2);
			bcCriteria.NextRecord();
			bcPartner.NextRecord();
		}		
		
	}
		
	public void RemoveCriteria(String partyId, String filter) throws Exception {
		RWJApplication app = new RWJApplication();
		BasicDBObject query = new BasicDBObject();
		
		RWJBusComp bcParty = app.GetBusObject("Party").getBusComp("Party");
		
		query.put("_id",new ObjectId(partyId));

		int nRecs = bcParty.ExecQuery(query);

		Long l_user_id = (Long) bcParty.GetFieldValue("MLID");
		
		query.clear();
		query.put("partyId", partyId);
		query.put("partyRelation", filter);

		RWJBusComp bcCriteria = app.GetBusObject("Criteria").getBusComp("Criteria");

		nRecs = bcCriteria.ExecQuery(query);

		for ( int i = 0 ; i < nRecs; i++) {

			Long l_item_id = (Long) bcCriteria.GetFieldValue("MLID");
			
			try {
				removePreference(l_user_id, l_item_id);
			    log.info("Remove Criteria: " + String.valueOf(l_user_id) + ":" + String.valueOf(l_item_id) );    
			} catch (TasteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			bcCriteria.NextRecord();
		}
	}
	
	public void RecalibrateMLIDs(String owner_id) throws Exception {

		BasicDBObject query = new BasicDBObject();
		try {
			
		   	BasicDBObject mlid =  new BasicDBObject();
	    	mlid.put("MLID", 0L);
	    	BasicDBObject update = new BasicDBObject();
			update.put("$set",mlid);

			db.getColl("rwCriteria").updateMulti(query, update);
			db.getColl("rwPartner").updateMulti(query, update);
		}
		catch(Exception e) {
			e.printStackTrace();
		}

		DBCursor cursor = db.getColl("rwCriteria").find(query);
		long l = 1;
	    while (cursor.hasNext()) {
	        DBObject a = cursor.next();
	        String GlobalVal = a.get("GlobalVal").toString();
	        
	        query.clear();
	        query.put("GlobalVal", GlobalVal);
	        DBObject b = db.getColl("rwCriteria").findOne(query);
	        
	        Long bl = (Long) b.get("MLID"); 
	        
	        if ( bl > 0L )
		        a.put("MLID", bl );
	        else 
	        	a.put("MLID", l++ );
	        
	        db.getColl("rwCriteria").save(a);
	    }

	    query.clear();
	    query.put("userId", owner_id);
	    
		cursor = db.getColl("rwPartner").find(query);
		// MLID = 1 is defined for the Target.
		l = 2;
	    while (cursor.hasNext()) {
	        DBObject a = cursor.next();
	        a.put("MLID", l++ );
	        db.getColl("rwPartner").save(a);
	    }
	}
}
