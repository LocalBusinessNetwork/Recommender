package com.rw.Recommender;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;

public class LeadFinderTest {
	
	private String userid;
	private String partyId;
	
	@Before
	public void setUp() throws Exception {
		RWJApplication app = new RWJApplication();
		
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		BasicDBObject query = new BasicDBObject();
		query.put("emailAddress", "phil@referralwiretest.biz"); 
		
        int nRecs = bc.ExecQuery(query);

        userid = bc.GetFieldValue("credId").toString();
        partyId = bc.GetFieldValue("_id").toString();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
        
        LeadFinder lf = new LeadFinder(userid);
        lf.FindLeadsForParty(partyId);
        //lf.FindLeadsForPartners();

	}

}
