package the.floow.challenge.dao;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.Date;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import the.floow.challenge.entity.DataSource;
import the.floow.challenge.enums.FileStatus;

public class FileDao extends GenericDao {

	public FileDao(DataSource dataSouce) {
		super(dataSouce);
	}

	private MongoCollection<Document> getFileCollection() {

		MongoDatabase database = this.getMongoDatabase();
		return database.getCollection("files");
	}

	public boolean isFileExist(String name) {
		MongoCollection<Document> collection = this.getFileCollection();
		Document file = collection.find(eq("name", name)).first();
		return file != null ? true : false;
	}

	public boolean isFileExist(String name, ObjectId executorID) {
		MongoCollection<Document> collection = this.getFileCollection();
		Document file = collection.find(and(eq("name", name), eq("executorID", executorID))).first();
		return file != null ? true : false;
	}

	public ObjectId getFileID(String name) {
		MongoCollection<Document> collection = this.getFileCollection();

		Document file = collection.find(and(eq("name", name), eq("status", FileStatus.PROCESSING.getValue()))).first();

		return file != null ? (ObjectId) file.get("_id") : null;
	}

	public ObjectId getFileID() {
		MongoCollection<Document> collection = this.getFileCollection();

		Document file = collection.find(eq("status", FileStatus.PROCESSING.getValue())).first();

		return file != null ? (ObjectId) file.get("_id") : null;
	}

	public ObjectId setFileStatusToProcessing(String name) {
		MongoCollection<Document> collection = this.getFileCollection();

		collection.updateMany(eq("name", name), new Document("$set",
				new Document("status", FileStatus.PROCESSING.getValue()).append("updatedTimestamp", new Date())));

		Document processingFile = collection.find(eq("status", FileStatus.PROCESSING.getValue()))
				.sort(new Document("_id", 1)).first();

		return (ObjectId) processingFile.get("_id");
	}
	public void updateFileStaus(ObjectId fileID, FileStatus status) {
		MongoCollection<Document> collection = this.getFileCollection();

		collection.updateMany(eq("_id", fileID), new Document("$set",
				new Document("status", status.getValue()).append("updatedTimestamp", new Date())));
	}
	public ObjectId create(String name, String filePath, ObjectId executorID) {
		MongoCollection<Document> collection = this.getFileCollection();

		Document doc = new Document("name", name).append("filePath", filePath).append("executorID", executorID)
				.append("status", FileStatus.CREATED.getValue()).append("createdTimestamp", new Date())
				.append("updatedTimestamp", new Date());

		collection.insertOne(doc);
		return (ObjectId) doc.get("_id");
	}
}
