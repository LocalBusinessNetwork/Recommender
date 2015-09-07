package com.rw.Recommender;

import static org.junit.Assert.*;

import java.util.Map;

import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.mongoStore;

public class LeadTrainer {
	private String userid;

	@Before
	public void setUp() throws Exception {
		RWJApplication app = new RWJApplication();
		
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		BasicDBObject query = new BasicDBObject();
		query.put("emailAddress", "phil@referralwiretest.biz"); 
		
        int nRecs = bc.ExecQuery(query);

        userid = bc.GetFieldValue("credId").toString();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		LeadRecommenderModel lrm = new LeadRecommenderModel();
		lrm.RecalibrateMLIDs(userid);
	}
}
