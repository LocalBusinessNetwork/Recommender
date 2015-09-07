package com.rw.Recommender;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;

public class DupFinderTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		RWJApplication app = new RWJApplication();
		
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		BasicDBObject query = new BasicDBObject();
		query.put("emailAddress", "phil@referralwiretest.biz"); 
		
        int nRecs = bc.ExecQuery(query);

        String userid = bc.GetFieldValue("credId").toString();
        
        DupFinder dr = new DupFinder(userid);
  
		dr.model.SetUpTrainingData();
        dr.FindDuplicates();
		dr.model.CleanUpModelData();

		dr.DebugPrintDuplicates();
	
	}

}
