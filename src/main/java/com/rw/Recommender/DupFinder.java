package com.rw.Recommender;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.mongoStore;

public class DupFinder {
	static final Logger log = Logger.getLogger(DupFinder.class.getName());
    
	static String counterBase = "DupFinder_";
    
	private DBCollection contactsDups;

	private RWJApplication app = new RWJApplication();
	private mongoStore db = new mongoStore();
	
	String onwer_id = null;
	DupRecommenderModel model = null;
	
	public DupFinder(String uid) throws Exception {
		onwer_id = uid;
		contactsDups = db.getColl("rwContactsDups");
		
		resetDups();
		model = new DupRecommenderModel(onwer_id);
	}
	
	public void FindDuplicates() throws Exception {

		DBCollection partners = db.getColl("rwPartner");
		resetDups();
		
		// Load training data into mahout Recommender
	    model.buildModel();
		RWRecommender r = new RWRecommender("pearson");
		r.build(model);

		BasicDBObject query = new BasicDBObject();

		query.put("userId", onwer_id );
		DBCursor cursor = partners.find(query);
			
	    while (cursor.hasNext()) {
	        Map<String,Object> person = (Map<String,Object>) cursor.next().toMap();

			
			String partyId = person.get("partnerId").toString();

			query.clear();
			query.put("owner_id", onwer_id );
			query.put("dup_id", partyId );
			DBObject dupExists = contactsDups.findOne(query);
			if ( dupExists != null ) continue;

			long uid = Long.parseLong(model.fromIdToLong(partyId));
			
			// Look for similar users
			ArrayList<ArrayList<String>> recos = r.recommendUsers(uid);
			if (recos.size() <= 0) continue;

			for ( int j = 0 ; j < 3  && j < recos.size() ; j++) {
				ArrayList<String> reco = recos.get(j);
				
				for ( int k = 0 ; k < reco.size() ; k++ ) {
					
					String item = reco.get(k);

					BasicDBObject dups = new BasicDBObject();
					dups.put("owner_id", onwer_id);
					dups.put("orig_id", partyId);
					dups.put("dup_id", item);
					contactsDups.insert(dups);
				}
			}
		}

	}
	
	void resetDups() {
		BasicDBObject query = new BasicDBObject();
		query.put("owner_id", onwer_id );		
		// cleanup previous run
		contactsDups.remove(query);
	}
	
	void DebugPrintDuplicates() throws Exception {
		
		DBCollection partyColl = db.getColl("rwParty");

		BasicDBObject query = new BasicDBObject();
		query.put("owner_id", onwer_id );
		
		DBCursor cursor = contactsDups.find(query);
		
	    while (cursor.hasNext()) {
	        Map<String,Object> dup = (Map<String,Object>) cursor.next().toMap();
			query.clear();
			query.put("_id",  new ObjectId(dup.get("orig_id").toString()));
			DBObject origObj = partyColl.findOne(query);
			
			query.clear();
			query.put("_id",  new ObjectId(dup.get("dup_id").toString()));
			
			DBObject dupObj = partyColl.findOne(query);
			
			log.info("Original :" + origObj.get("fullName").toString() + " Duplicate: " + dupObj.get("fullName").toString());
	    }
	    
	}
	
}
