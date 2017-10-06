package the.floow.challenge.dao;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.ne;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import the.floow.challenge.entity.BlockQueue;
import the.floow.challenge.entity.DataSource;
import the.floow.challenge.enums.BlockStatus;

public class FileBlockDao extends GenericDao {

	public FileBlockDao(DataSource dataSouce) {
		super(dataSouce);
	}
	private MongoCollection<Document> getFileBlockCollection(){		
		MongoDatabase database = this.getMongoDatabase();
		return database.getCollection("files_blocks");		
	}
	public void createFileBlocks(ObjectId fileID, long blockSize, long fileSize){
		MongoCollection<Document> collection = this.getFileBlockCollection();

		List<Document> blockList = new ArrayList<Document>();
		long numSplits = fileSize / blockSize;
		if (fileSize % blockSize == 1) {
			numSplits++;
		}
		
		for (int i = 0; i < numSplits; i++) {
			Document block = new Document("name", "FILE_BLOCK_"+(i+1))
					.append("fileID", fileID)
					.append("blockNo", i+1)
					.append("executorID", "")
					.append("status", BlockStatus.AVAILABLE.getValue())					
					.append("createdTimestamp", new Date())
					.append("updatedTimestamp", new Date());
			blockList.add(block);
		}
		collection.insertMany(blockList);
	}
	public List<BlockQueue> getAllBlocks(){
		MongoCollection<Document> collection = this.getFileBlockCollection();

		MongoCursor<Document>  docsCursor = collection.find().iterator();
		List<BlockQueue> queue = new ArrayList<>();
		try {
		    while (docsCursor.hasNext()) {
		    	Document doc = docsCursor.next();
		    	queue.add(new BlockQueue(doc.getInteger("name"), doc.getString("status")));
		    }
		} finally {
			docsCursor.close();
		}
		
		return queue;
	}
	public long getAllBlockCount(ObjectId fileID){
		return this.getFileBlockCollection().count(and(eq("fileID", fileID)));
	}
	public long getFileAvailableBlockCount(ObjectId fileID){
		return this.getFileBlockCollection().count(and(eq("fileID", fileID), eq("status", BlockStatus.AVAILABLE.getValue())));
	}
	public List<Integer> getFileAvailableBlocks(ObjectId fileID){
		MongoCollection<Document> collection = this.getFileBlockCollection();

		MongoCursor<Document>  docsCursor = collection.find(and(eq("fileID", fileID), eq("status", BlockStatus.AVAILABLE.getValue()))).iterator();
		List<Integer> blockNos = new ArrayList<>();
		try {
		    while (docsCursor.hasNext()) {
		    	Document doc = docsCursor.next();
		    	Integer blockNo= doc.getInteger("blockNo");
		    	blockNos.add(blockNo);
		    }
		} finally {
			docsCursor.close();
		}
		
		return blockNos;
	}
	public List<Integer> getFileBlocksByStatus(ObjectId fileID, BlockStatus status){
		MongoCollection<Document> collection = this.getFileBlockCollection();

		MongoCursor<Document>  docsCursor = collection.find(and(eq("fileID", fileID), eq("status", status))).iterator();
		List<Integer> blockNos = new ArrayList<>();
		try {
		    while (docsCursor.hasNext()) {
		    	Document doc = docsCursor.next();
		    	Integer blockNo= doc.getInteger("blockNo");
		    	blockNos.add(blockNo);
		    }
		} finally {
			docsCursor.close();
		}
		
		return blockNos;
	}
	public long getBlockCountByStatus(ObjectId fileID, BlockStatus status){
		return  this.getFileBlockCollection().count((and(eq("fileID", fileID), eq("status", status.getValue()))));
	}
	public long getBlockCountNotWritten(ObjectId fileID){
		return  this.getFileBlockCollection().count((and(eq("fileID", fileID), ne("status", BlockStatus.WRITTEN.getValue()))));
	}
	public void updateBlockStatus(ObjectId fileID, ObjectId executorID, int blockNo, BlockStatus status){
		MongoCollection<Document> collection = this.getFileBlockCollection();
	
		Document setParams = new Document("$set", 
									new Document("status", status.getValue())
										.append("executorID", executorID)
										.append("updatedTimestamp", new Date()));
		
	 	collection.updateOne(and(eq("fileID", fileID), eq("blockNo", blockNo)), setParams);
	}
	public void updateBlockStatus(ObjectId fileID, List<Integer> blockNos, BlockStatus status){
		MongoCollection<Document> collection = this.getFileBlockCollection();
	 	collection.updateMany(and(eq("fileID", fileID), in("blockNo", blockNos)),new Document("$set", new Document("status", status.getValue()).append("updatedTimestamp", new Date())));
	}
}
