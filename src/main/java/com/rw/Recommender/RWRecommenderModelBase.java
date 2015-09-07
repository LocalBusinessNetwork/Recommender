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
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

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
import com.rw.persistence.JedisMT;
import com.rw.persistence.mongoStore;

public class RWRecommenderModelBase implements DataModel {
	static final Logger log = Logger.getLogger(RWRecommenderModelBase.class.getName());
	
	public DataModel delegate;
	public DBCollection dbModel;
	public DBCollection dbIdMap;
	public DBCollection dbItemMap;
	public String owner_id = null;
	public String classifier = "Default_";

	public static mongoStore db = new mongoStore();

	public static JedisMT jedisMt = new JedisMT();

	public RWRecommenderModelBase () {
	}
	
	public RWRecommenderModelBase (String owner_id, String counterBase) throws Exception {
	    this.owner_id = owner_id;
	    this.classifier = counterBase;
		dbItemMap = db.getColl("rwMlItemMap");
		dbModel = db.getColl("rwMlModel");
		dbIdMap = db.getColl("rwMlIdMap");
	}

	public void RemoveUserDataFromModel(String partyId) {

		BasicDBObject query = null;
		
		query = new BasicDBObject();
		query.put("owner_id", owner_id);
		query.put("element_id", partyId);
		query.put("classifier", classifier);
	    
		dbIdMap.remove(query);
	    
	    query.clear();
		query.put("owner_id", owner_id);
		query.put("party_id", partyId);
		query.put("classifier", classifier);

		dbModel.remove(query);
	    
		query.clear();
		query.put("owner_id", owner_id);
		query.put("party_id", partyId);
		query.put("classifier", classifier);
		
		dbItemMap.remove(query);

	}

	
	public void CleanUpModelData() {

		BasicDBObject query = new BasicDBObject();
		query.put("owner_id", owner_id);
		query.put("classifier", classifier);

		dbItemMap.remove(query);
		dbModel.remove(query);
		dbIdMap.remove(query);
		
	}

	public String fromIdToLong(String id) {

		BasicDBObject query = new BasicDBObject();
		
		query.put("owner_id", owner_id);
		query.put("element_id", id);
		query.put("classifier", classifier);
	
		DBObject objectIdLong = dbIdMap.findOne(query);
		
	    if (objectIdLong != null) {
	      Map<String,Object> idLong = (Map<String,Object>) objectIdLong.toMap();
	      return (String) idLong.get("long_value");
	    } else {
	      objectIdLong = new BasicDBObject();
	      String longValue = Long.toString(jedisMt.getCounter(classifier,owner_id));
	      objectIdLong.put("owner_id", owner_id);
	      objectIdLong.put("element_id", id);
	      objectIdLong.put("long_value", longValue);
	      objectIdLong.put("classifier", classifier);
	      dbIdMap.insert(objectIdLong);
	      return longValue;
	    }
	}
	
	public String fromLongToId(long id) {

		BasicDBObject query = new BasicDBObject();
		
		query.put("owner_id", owner_id);
		query.put("long_value", Long.toString(id));
		query.put("classifier", classifier);

		DBObject objectIdLong = dbIdMap.findOne(query);
	    Map<String,Object> idLong = (Map<String,Object>) objectIdLong.toMap();
	    return (String) idLong.get("element_id");
	}

	public float getPreference(Object value) {
	    if (value != null) {
	      if (value.getClass().getName().contains("String")) {
	        return Float.parseFloat((String) value);
	      } else {
	        return Double.valueOf(value.toString()).floatValue();
	      }
	    } else {
	      return 0.5f;
	    }
	}
	
	
	public void AddItem(String user_id, String criteria, float pref) {
    	
    	String item_id = getItemId(user_id,criteria);

    	if ( item_id == null || item_id.isEmpty() ) return;

    	// Add criteria to the Lead Model
		BasicDBObject modelItem = new BasicDBObject();
		
		modelItem.put("owner_id", owner_id);
	    modelItem.put("user_id", user_id);
	    modelItem.put("item_id", item_id);
	    modelItem.put("classifier", classifier);
	    modelItem.put("score", pref);
	     
	    dbModel.insert(modelItem);
	
    }

	public String getItemId(String user_id, String item_name) {
		
	 	if ( item_name == null || item_name.isEmpty() ) return null;
	    	
	    	BasicDBObject query = new BasicDBObject();
	    	query.put("owner_id", owner_id);
	    	query.put("item_name", item_name);
	    	
	    	DBObject p = dbItemMap.findOne(query);
	    	String item_id = null;
	    	if ( p == null ) {
	    		// Add criteria to the model
	    		BasicDBObject q = new BasicDBObject();
		    	q.put("owner_id", owner_id);
		    	q.put("user_id", user_id);
		    	q.put("item_name", item_name);
			    q.put("classifier", classifier);
		    	dbItemMap.save(q);
		    	
		    	// add criteria to ML ID lookup table
		    	item_id = q.getString("_id").toString();
		    	BasicDBObject objectIdLong = new BasicDBObject();
			    String longValue = Long.toString(jedisMt.getCounter(classifier,owner_id));
			    objectIdLong.put("owner_id", owner_id);
			    objectIdLong.put("element_id", item_id);
			    objectIdLong.put("long_value", longValue);
			    objectIdLong.put("classifier", classifier);

			    dbIdMap.insert(objectIdLong);
			    
	    	}
	    	else {
	    		item_id = p.get("_id").toString();
	    	}
		
	    	return item_id;
	}
	
	public void buildModel() {
		
		jedisMt.resetCounter(classifier, owner_id);

		FastByIDMap<Collection<Preference>> preferenceMap = new FastByIDMap<Collection<Preference>>();
	
		BasicDBObject query = new BasicDBObject();
		query.put("owner_id", owner_id);
		query.put("classifier", classifier);

		DBCursor cursor = dbModel.find(query);
		
	    while (cursor.hasNext()) {
	        Map<String,Object> lead = (Map<String,Object>) cursor.next().toMap();
	        
	        long l_user_id = Long.parseLong(fromIdToLong(lead.get("user_id").toString()));
	        long l_item_id = Long.parseLong(fromIdToLong(lead.get("item_id").toString()));
	        float f_score = getPreference(lead.get("score"));
	        
	        Collection<Preference> preferences = preferenceMap.get(l_user_id);
	        
	        if (preferences == null) {
	        	preferences = Lists.newArrayListWithCapacity(2);
	        	preferenceMap.put(l_user_id, preferences);
	        }
	        preferences.add(new GenericPreference(l_user_id, l_item_id, f_score));
	        log.info(String.valueOf(l_user_id) + ":" + String.valueOf(l_item_id) + ":" + String.valueOf(f_score));    
	        
	    }
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
		 delegate.removePreference(partyId,actionId);
	}

	public void setPreference(long partyId, long actionId, float preference)
			throws TasteException {
		 delegate.setPreference(partyId,actionId, preference );
	}

}
