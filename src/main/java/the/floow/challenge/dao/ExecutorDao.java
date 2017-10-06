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
                .append("createdTimestamp", new Date());
		
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
		    	executors.add(new Executor(doc.getString("name"), doc.getString("status")));
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
	public void updateExectorStatus(ObjectId executorID, ExecutorStatus status){
		MongoCollection<Document> collection = this.getExecutorCollection();
	 	collection.updateOne(eq("_id", executorID),new Document("$set", new Document("status", status.getValue()).append("updatedTimestamp", new Date())));
	}
}
