package the.floow.challenge.dao;

import static com.mongodb.client.model.Filters.eq;

import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;

import the.floow.challenge.entity.DataSource;
import the.floow.challenge.entity.QueueMessage;
import the.floow.challenge.enums.MessageQueueStatus;
/**
This class is managing queue in mongoDB
@author Wajid */
public class MongoMessageQueue extends GenericDao {

	private final String collectionName = "messages";

	public MongoMessageQueue(DataSource dataSouce) {
		super(dataSouce);
	}
	private MongoCollection<Document> getMessageQueueCollection(){
		
		MongoDatabase database = this.getMongoDatabase();
		return database.getCollection(collectionName);		
	}
	/**
		This function enqueues the data into message collection

		@param message the input parameter as QueueMessage Entity
	 */
	public void enqueue(QueueMessage message) {
		
		MongoCollection<Document> collection = this.getMessageQueueCollection();
		Document doc = new Document("blockNo", message.blockNo)
							.append("data", message.data)
							.append("status", MessageQueueStatus.AVAILABLE.getValue());

		collection.insertOne(doc);		
	}
	/**
		This function atomically dequeues the data into message collection
	
		@return the first available QueueMessage
	 */
	public QueueMessage dequeue() {

		MongoCollection<Document> collection = this.getMessageQueueCollection();
	
		Document doc = collection.findOneAndUpdate (
					eq("status", MessageQueueStatus.AVAILABLE.getValue()), 
                    new Document("$set", new Document("status", MessageQueueStatus.PROCESSED.getValue())),
                    new FindOneAndUpdateOptions().sort(new Document("_id",1))
				); 

		// delete all blocks which are processed
		collection.deleteMany(eq("status", MessageQueueStatus.PROCESSED.getValue()));
		
		QueueMessage message = null;
		if (doc != null) {
			Integer blockNo = doc.getInteger("blockNo");
			byte[] data = ((Binary) doc.get("data")).getData();
			message = new QueueMessage(blockNo, data);
		}		
		return message;
	}
	/**
		This function returns the size of the queue
		
		@return size as long
	 */
	public long size() {
		return this.getMessageQueueCollection().count();
	}
	/**
		This function checks whether the queue is empty or not. 
		
		@return is empty as boolean
	 */
	public boolean empty() {
		return this.getMessageQueueCollection().count() == 0 ? true : false;
	}
}
