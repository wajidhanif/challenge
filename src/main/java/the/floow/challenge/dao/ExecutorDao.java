package the.floow.challenge.dao;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;

import the.floow.challenge.entity.DataSource;
import the.floow.challenge.entity.Executor;
import the.floow.challenge.enums.ExecutorStatus;

public class ExecutorDao extends GenericDao {
	
	
	public ExecutorDao(DataSource dataSouce) {
		super(dataSouce);
	}
	private MongoCollection<Document> getExecutorCollection(){		
		MongoDatabase database = this.getMongoDatabase();
		return database.getCollection("executor");		
	}
	
	public ObjectId create(String name){
		MongoCollection<Document> collection = this.getExecutorCollection();
		
		Document excDoc = new Document("name", name)
                .append("status", ExecutorStatus.LIVE.getValue())
                .append("isServer", 0)
                .append("createdTimestamp", new Date())
                .append("updatedTimestamp", new Date())
                .append("runningTimestamp", new Date());
		
		collection.insertOne(excDoc);	
		ObjectId executorID = (ObjectId)excDoc.get( "_id" );
		return executorID;
	}
	public long getExecutorCount(){
		return this.getExecutorCollection().count();
	}
	public List<Executor> getAllExecutor(){
		MongoDatabase database = this.getMongoDatabase();

		MongoCollection<Document> collection = database.getCollection("executor");

		MongoCursor<Document>  docsCursor = collection.find().iterator();
		List<Executor> executors = new ArrayList<>();
		try {
		    while (docsCursor.hasNext()) {
		    	Document doc = docsCursor.next();
		    	executors.add(new Executor(doc.getObjectId("_id"), doc.getString("name"), doc.getString("status"), doc.getDate("runningTimestamp")));
		    }
		} finally {
			docsCursor.close();
		}
		
		return executors;
	}
	public ObjectId getExecutor(String name){
		MongoCollection<Document> collection = this.getExecutorCollection();
		Document executor = collection.find(eq("name", name)).first();
		ObjectId id = null;
		if(executor!=null){
			id = (ObjectId)executor.get( "_id" );
		}
		return id;
	}
	public Executor getExecutor(ObjectId id){
		MongoCollection<Document> collection = this.getExecutorCollection();
		Document doc = collection.find(eq("_id", id)).first();
		Executor executor = null;
		if(doc!=null){
			executor  = new Executor(doc.getObjectId("_id"), doc.getString("name"), doc.getString("status"), doc.getDate("runningTimestamp"));
		}
		return executor;
	}
	
	public void updateExectorStatus(ObjectId executorID, ExecutorStatus status){
		MongoCollection<Document> collection = this.getExecutorCollection();
	 	collection.updateOne(eq("_id", executorID),new Document("$set", new Document("status", status.getValue()).append("updatedTimestamp", new Date())));
	}

	public ObjectId updateExecutorAsServer() {
		MongoCollection<Document> collection = this.getExecutorCollection();	
		Document doc = collection.findOneAndUpdate (
					eq("isServer", 0), 
                    new Document("$set", new Document("isServer", 1)),
                    new FindOneAndUpdateOptions().sort(new Document("_id",1))
				); 

		ObjectId id = null;
		if(doc!=null){
			id = (ObjectId)doc.get( "_id" );
		}
		return id;
	}
	
	public Executor getExecutorAsServer(){
		MongoCollection<Document> collection = this.getExecutorCollection();
		Document doc = collection.find(eq("isServer", 1)).first();
		Executor executor = null;
		if(doc!=null){
			executor  = new Executor(doc.getObjectId("_id"), doc.getString("name"), doc.getString("status"), doc.getDate("runningTimestamp"));
		}
		return executor;
	}
	public void updateExectorRunningTime(ObjectId executorID){
		MongoCollection<Document> collection = this.getExecutorCollection();
	 	collection.updateOne(eq("_id", executorID),new Document("$set", new Document("runningTimestamp", new Date()).append("status", ExecutorStatus.LIVE.getValue())));
	}	
	public void updateExectorServerInfo(ObjectId executorID){
		MongoCollection<Document> collection = this.getExecutorCollection();
	 	collection.updateOne(eq("_id", executorID),new Document("$set", new Document("isServer", 0).append("status", ExecutorStatus.STOP.getValue())));
	}	
	public void deleteExector(ObjectId executorID){
		MongoCollection<Document> collection = this.getExecutorCollection();
	 	collection.deleteOne(eq("_id", executorID));
	}	
}
