package com.rw.Recommender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.PlusAnonymousUserDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.bson.types.ObjectId;
import org.json.JSONArray;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.mongoStore;

public class LeadFinder {
	static final Logger log = Logger.getLogger(LeadFinder.class.getName());

	private RWJApplication app = new RWJApplication();
	private mongoStore db = new mongoStore();
	private String owner_id = null;

	public LeadFinder(String owner_id) {
		this.owner_id = owner_id;
	}

	public void FindLeadsForParty(String partyId) throws Exception {

		
		LeadRecommenderModel model = new LeadRecommenderModel();

		int totalCriteriaSpec = model.AddCriteria(1L, "TARGET", partyId);
		model.SetUpDescriptors(owner_id, partyId);
		model.buildModel();

		PreferenceArray a = null;

		try {
			a = model.getPreferencesFromUser(1L);
		} catch (TasteException e1) {
			e1.printStackTrace();
			return;
		}

		LeadRecommender r = new LeadRecommender();
		r.build(model, 10);

	    try {
			long[] recomendations = r.recommender.mostSimilarUserIDs(1L, 10);

			for ( int i = 0 ; i < recomendations.length; i++) {
				PreferenceArray b =  model.getPreferencesFromUser(recomendations[i]);
				log.info("Matching Item for Party 1 :" + 1 + " & Party 2 : " + recomendations[i]);
				long totalCriteriaMet = 0L;
				
				JSONArray CriteriaMet = new JSONArray();

				BasicDBObject rec = new BasicDBObject();
				
				rec.put("memberId", owner_id); // originator
				rec.put("partyId", partyId); // Receiver
				String contactId = GetUserIdFromMLID(recomendations[i] );
				rec.put("contactId", contactId); // Target
				
				rec.put("totalCriteriaSpec", totalCriteriaSpec); // Total Criteria that need to met.
				
				for ( int j = 0; j < a.length(); j++) 
					if ( b.hasPrefWithItemID(a.getItemID(j)) ) {
						totalCriteriaMet++;

						BasicDBObject query = new BasicDBObject();
						RWJBusComp bcCriteria = app.GetBusObject("Criteria").getBusComp("Criteria");
						query.put("MLID", a.getItemID(j) );
						int nCriteria = bcCriteria.ExecQuery(query);

						String GlobalVal = bcCriteria.GetFieldValue("GlobalVal").toString();
						String category = bcCriteria.GetFieldValue("category").toString();

						String tagBase = "cat" + String.valueOf(j+1);
						
						rec.put(tagBase + "Name", category);
						rec.put(tagBase + "CriteriaName", GlobalVal);
						rec.put(tagBase + "CriteriaMet", 1);

						log.info( "Descriptor/Target Id:" + a.getItemID(j) );
						
					}

				rec.put("totalCriteriaMet", totalCriteriaMet); // Total Criteria met for this contact 
				rec.put("type","CUST_FOR_PART" ); // Total Criteria met for this contact 
				rec.put("status", "UNREAD");
				rec.put("score", (Long) (totalCriteriaMet/totalCriteriaSpec));

				DBCollection recs = db.getColl("rwRecommendations");
				recs.save(rec);
				
			}

	    } catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void FindLeadsForPartners() throws Exception {

		RWJBusComp bcPartner = app.GetBusObject("Partner").getBusComp("Partner");
		
		BasicDBObject query = new BasicDBObject();

		query.clear();
		query.put("userId", owner_id);
		query.put("type", "REFERRAL_PARTNER");
		
		int nRecs = bcPartner.ExecQuery(query);

		for ( int i = 0; i < nRecs ; i++) {
			String partnerId = (String) bcPartner.GetFieldValue("partnerId");
			FindLeadsForParty(partnerId);
			bcPartner.NextRecord();
		}
	}
	
	String GetUserIdFromMLID(long mlid) throws Exception {
		RWJBusComp bcPartner = app.GetBusObject("Partner").getBusComp("Partner");
		BasicDBObject query = new BasicDBObject();
		query.put("MLID", mlid );

		int nRecs = bcPartner.ExecQuery(query);
		String id = bcPartner.GetFieldValue("partnerId").toString();
		
		return id;
	}

	String GetGlobalValFromMLID(long mlid) throws Exception {
		RWJBusComp bcCriteria = app.GetBusObject("Criteria").getBusComp("Criteria");
		BasicDBObject query = new BasicDBObject();
		query.put("MLID", mlid );

		int nRecs = bcCriteria.ExecQuery(query);
		String GlobalVal = bcCriteria.GetFieldValue("GlobalVal").toString();
		return GlobalVal;
	}

}
