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

public class RWRecommender implements Recommender {
		static final Logger log = Logger.getLogger(RWRecommender.class.getName());

	  /**
	   * Default user threshold
	   */
	  protected static double defaultUserThreshold = 0.50;
	  
	  /**
	   * Default user similarity method
	   */
	  protected static String defaultSimilarityMeasure = "euclidean";
	  
	  /**
	   * Default neighborhood type
	   */
	  protected static String defaultNeighborhoodType = "nearest"; // "threshold"; // "nearest";
	  
	  /**
	   * Default number of Neighbors
	   */
	   protected static int defaultNeighborsNumber = 10;
	   
	   /**
	    * Default maximum number of recommendations
	    */
	   protected static int defaultMaxRecommendations = 100;

	  /**
	   * User threshold
	   */
	  protected double userThreshold = defaultUserThreshold;
	  
	  /**
	   * Neighborhood type
	   */
	  protected String neighborhoodType = defaultNeighborhoodType;
	  
	  /**
	   * User similarity method
	   */
	  protected String similarityMeasure = defaultSimilarityMeasure;

	  /**
	   * Number of Neighbors
	   */
	   protected int neighborsNumber = defaultNeighborsNumber;

	  /**
	   * Maximum number of recommendations
	   */
	  protected int maxRecommendations = defaultMaxRecommendations;
	  
	  

	  /**
	   * Recommender variables
	   */
	  protected RWRecommenderModelBase dataModel;
	  protected static UserSimilarity similarity;
	  protected static UserNeighborhood neighborhood;
	  protected static GenericUserBasedRecommender recommender;

	  public RWRecommender(String sim){
		  if ( sim != null )
			  similarityMeasure = sim;
	  }
	  
	  public void build(RWRecommenderModelBase dataModel) {
		    try {
		      this.dataModel = dataModel;
		      if (similarityMeasure.equals("log")) {
		        similarity = new LogLikelihoodSimilarity(this.dataModel);
		      } else if (similarityMeasure.equals("pearson"))  {
		        similarity = new PearsonCorrelationSimilarity(this.dataModel);
		      } else if (similarityMeasure.equals("spearman"))  {
		        similarity = new SpearmanCorrelationSimilarity(this.dataModel);
		      } else if (similarityMeasure.equals("tanimoto")) {
		        similarity = new TanimotoCoefficientSimilarity(this.dataModel);
		      } else {
		        similarity = new EuclideanDistanceSimilarity(this.dataModel);
		      }
		      if (neighborhoodType.equals("threshold")) {
		        neighborhood = new ThresholdUserNeighborhood(userThreshold,similarity, this.dataModel);
		      } else {
		        neighborhood = new NearestNUserNeighborhood(neighborsNumber,similarity, this.dataModel);
		      }
		      recommender = new GenericUserBasedRecommender(this.dataModel, neighborhood, similarity);
		    } catch (Exception e) {
		      log.debug("Error while starting recommender.", e);
		    }
	  }  


	  public ArrayList<ArrayList<String>> recommendUsers(long id) {
		    ArrayList<ArrayList<String>> result = null;
		    try {
		      long[] recomendations = recommender.mostSimilarUserIDs(id, maxRecommendations);
		      result = new ArrayList<ArrayList<String>>();
		      for (long r : recomendations) {
		        ArrayList<String> user = new ArrayList<String>();
		        user.add(dataModel.fromLongToId(r));
		        result.add(user);
		      }
		    } catch (TasteException e) {
		      log.error("Error while processing user recommendations for user " + id, e);
		    }
		    return result;
	  }

	  public ArrayList<ArrayList<String>> recommendItems(long id) {
		    ArrayList<ArrayList<String>> result = null;
		    try {
		      List<RecommendedItem> recomendations = recommender.recommend(id, maxRecommendations);
		      result = new ArrayList<ArrayList<String>>();
		      for (RecommendedItem r : recomendations) {
		        ArrayList<String> item = new ArrayList<String>();
		        item.add(dataModel.fromLongToId(r.getItemID()));
		        item.add(Float.toString(r.getValue()));
		        result.add(item);
		      }
		    } catch (TasteException e) {
		      log.error("Error while processing item recommendations for user " + id, e);
		    }
		    return result;
	  }

	  
	  public void refresh(Collection<Refreshable> arg0) {
		// TODO Auto-generated method stub

	  }

	public float estimatePreference(long arg0, long arg1) throws TasteException {
		// TODO Auto-generated method stub
		return 0;
	}

	public DataModel getDataModel() {
		// TODO Auto-generated method stub
		return dataModel;
	}

	public List<RecommendedItem> recommend(long arg0, int arg1)
			throws TasteException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<RecommendedItem> recommend(long arg0, int arg1, IDRescorer arg2)
			throws TasteException {
		// TODO Auto-generated method stub
		return null;
	}

	public void removePreference(long arg0, long arg1) throws TasteException {
		// TODO Auto-generated method stub

	}

	public void setPreference(long arg0, long arg1, float arg2)
			throws TasteException {
		// TODO Auto-generated method stub

	}

}
