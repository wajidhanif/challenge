package the.floow.challenge.dao;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.MapReduceAction;

import the.floow.challenge.entity.DataSource;

public class WordsDao extends GenericDao {
	
	private final String wordsCollectionName = "words";
	private final String wordCountscollectionName = "wordCounts";

	public WordsDao(DataSource dataSouce) {
		super(dataSouce);
	}
	private MongoCollection<Document> getWordCollection(){		
		MongoDatabase database = this.getMongoDatabase();
		return database.getCollection(this.wordsCollectionName);		
	}
	public void create(ObjectId fileID, ObjectId executorID, ConcurrentHashMap<String, Long> words) {
		MongoCollection<Document> wordsCollection = this.getWordCollection();
		List<Document> wordList = new ArrayList<Document>();

		Date now = new Date();
		words.forEach((key, val) -> {
			Document word = new Document("FileID", fileID).append("executorID", executorID).append("word", key)
					.append("counts", val.intValue()).append("createdTimestamp", now);
			wordList.add(word);
		});

		wordsCollection.insertMany(wordList);
		
		String map ="function() {"+
                "var key = {"+
                              "FileID: this.FileID,"+
                              "word: this.word"+
                             "};"+
                "var value = this.counts;"+
                "emit( key, value);"+
                
            "}";

		String reduce = "function(key, values) {"+
                  "var counts= 0;"+
                  "values.forEach( function(value) {"+
                                        "counts += value;"+
                                  "}"+
                                ");"+
                  "return counts;"+
               "}";

		wordsCollection.mapReduce(map, reduce).filter(and(eq("executorID",executorID),eq("createdTimestamp",now))).action(MapReduceAction.REDUCE).collectionName(this.wordCountscollectionName).first();
		// remove all words computed by executor		
		//wordsCollection.deleteMany(eq("executorID",executorID)); 
	}
}
