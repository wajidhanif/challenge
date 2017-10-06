package the.floow.challenge.dao;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import the.floow.challenge.entity.DataSource;

public class GenericDao {

	private static MongoClient mongoClient = null;
	public DataSource dataSouce;

	public GenericDao(DataSource dataSouce) {
		this.dataSouce = dataSouce;
	}

	public MongoClient getMongoClient() {
		if (mongoClient == null) {
			mongoClient = new MongoClient(this.dataSouce.getIp(), this.dataSouce.getPort());
		}
		return mongoClient;
	}

	public MongoDatabase getMongoDatabase() {
		return this.getMongoClient().getDatabase(this.dataSouce.getDbName());
	}

}
