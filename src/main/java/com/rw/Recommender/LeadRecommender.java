package com.rw.Recommender;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.SpearmanCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.mongoStore;

public class LeadRecommender implements Recommender {
	  
	static final Logger log = Logger.getLogger(LeadRecommender.class.getName());
	protected LeadRecommenderModel dataModel;
	protected GenericUserBasedRecommender recommender;

	public LeadRecommender(){
	}
	  
	public void build(LeadRecommenderModel dataModel, int neighborsNumber) {
	    try {
	      this.dataModel = dataModel;
	      UserSimilarity similarity = new EuclideanDistanceSimilarity(this.dataModel);
	      UserNeighborhood neighborhood = new NearestNUserNeighborhood(neighborsNumber,similarity, this.dataModel);
	      recommender = new GenericUserBasedRecommender(this.dataModel, neighborhood, similarity);
	    } catch (Exception e) {
	      log.debug("Error while starting recommender.", e);
	    }
	}  
	  
	public void refresh(Collection<Refreshable> arg0) {
	}

	public float estimatePreference(long arg0, long arg1) throws TasteException {
		return 0;
	}

	public DataModel getDataModel() {
		return dataModel;
	}

	public List<RecommendedItem> recommend(long arg0, int arg1)
			throws TasteException {
		return null;
	}

	public List<RecommendedItem> recommend(long arg0, int arg1, IDRescorer arg2)
			throws TasteException {
		return null;
	}

	public void removePreference(long arg0, long arg1) throws TasteException {
		recommender.removePreference(arg0, arg1);
	}

	public void setPreference(long arg0, long arg1, float arg2)
			throws TasteException {
		recommender.setPreference(arg0, arg1, arg2);
	}

}
