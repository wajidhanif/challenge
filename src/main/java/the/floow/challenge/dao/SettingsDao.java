package the.floow.challenge.dao;

import static com.mongodb.client.model.Filters.eq;

import java.util.Date;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import the.floow.challenge.entity.DataSource;
import the.floow.challenge.enums.ExecutorStatus;

public class SettingsDao extends GenericDao {

	public SettingsDao(DataSource dataSouce) {
		super(dataSouce);
	}
	private MongoCollection<Document> getSettingsCollection(){		
		MongoDatabase database = this.getMongoDatabase();
		return database.getCollection("settings");		
	}
	public String getSetting(String name){
		MongoCollection<Document> collection = this.getSettingsCollection();
		
	 	Document file = collection.find(eq("name", name)).first();
	 	
	 	return file.getString("value");
	}
	public void create(String name, String value){
		MongoCollection<Document> collection = this.getSettingsCollection();
		
		Document excDoc = new Document("name", name)
				.append("value", value)
				.append("createdTimestamp", new Date())
                .append("updatedTimestamp", new Date());
		// if setting not exist, then create
		if(collection.count(eq("name", name))==0){
			collection.insertOne(excDoc);
		}
	}
}
