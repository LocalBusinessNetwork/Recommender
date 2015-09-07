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
import com.rw.persistence.mongoStore;

public class DupRecommenderModel extends RWRecommenderModelBase {

	static final Logger log = Logger.getLogger(DupRecommenderModel.class.getName());

	private final ReentrantLock reloadLock;

	public DupRecommenderModel () {
		this.reloadLock = new ReentrantLock();
	}
	
	public DupRecommenderModel (String owner_id) throws Exception {
		super(owner_id, "DupFinder_");
		this.reloadLock = new ReentrantLock();
	}
	
	@Override
	public String toString() {
	    return "mlDupFinderModel";
	}
	
	public void AddUserDataToModel(RWJBusComp bc) {

		String user_id = bc.currentRecord.get("partnerId").toString();
		
    	BasicDBObject query = new BasicDBObject();
    	query.put("owner_id", owner_id);
    	query.put("element_id", user_id);
		query.put("classifier", super.classifier);
   	
    	DBObject p = dbIdMap.findOne(query);

    	String oid = null;
    	if ( p == null ) {
		    String longValue = Long.toString(jedisMt.getCounter(super.classifier,owner_id));
		    query.put("long_value", longValue);
		    dbIdMap.insert(query);
    	}
    	
    	AddItem(user_id, bc.currentRecord.getString("firstName").toLowerCase(), (float) 0.99);
    	AddItem(user_id, bc.currentRecord.getString("emailAddress").toLowerCase(), (float) 0.95);
    	AddItem(user_id, bc.currentRecord.getString("gender"), (float) 0.90);
    	AddItem(user_id, extractPhone(bc.currentRecord.getString("mobilePhone")), (float) 0.85);
    	AddItem(user_id, extractPhone(bc.currentRecord.getString("homePhone")), (float) 0.85);
    	AddItem(user_id, bc.currentRecord.getString("streetAddress_work"), (float) 0.80);
    	AddItem(user_id, bc.currentRecord.getString("cityAddress_work"), (float) 0.80);
    	AddItem(user_id, bc.currentRecord.getString("stateAddress_work"), (float) 0.80);
    	AddItem(user_id, bc.currentRecord.getString("postalCodeAddress_work"), (float) 0.80);
    	AddItem(user_id, bc.currentRecord.getString("lastName").toLowerCase(), (float) 0.55);

	}

	public String extractPhone(String phone) {
		
		String santizedPhone = new String();
		for (int i =0; i < phone.length(); i++) {
			char c = phone.charAt(i);
			if ( c >= '0' && c <= '9') {
				santizedPhone += c;
			}
		}
		return santizedPhone;
	}
	
	public void SetUpTrainingData() throws Exception {
		
		CleanUpModelData();
		RWJApplication app = new RWJApplication();

		RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");
		BasicDBObject query = new BasicDBObject();
		query.put("userId", owner_id);

		int user_seed_limit = bc.ExecQuery(query);
		BasicDBObject objectIdLong = null;
		
		for ( int i = 0 ; i < user_seed_limit; i++) {
			AddUserDataToModel(bc);
		    bc.NextRecord();
		}

	}

}
