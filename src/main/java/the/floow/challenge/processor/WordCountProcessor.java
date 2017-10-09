package the.floow.challenge.processor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import the.floow.challenge.utils.Util;
/**
This class is processing file-block and computing word counts
@author Wajid */
public class WordCountProcessor implements CountProcessor {

	/**
	This function receives the file-block data as byte [] and performs
	following operations
	1. Converts the byte [] to list of lines
	2. Start streaming the lines
	3. Split line into individual words
	4. Convert stream of String[] to stream of String
	5. Convert to parallel stream
	6. Filter out non-word items
	7. Convert to lower case
	8. Use an AtomicAdder to tally word counts
	9. If a hashmap entry for the word doesn't exist yet Create a new LongAdder
	10.Increment the LongAdder for each instance of a word
	
	@param data the input parameter as byte []
    @param cProcessor the count processor object (Word/Number/Line) */
	
	@Override
	public ConcurrentHashMap<String, LongAdder> counts(byte[] data) throws IOException {
		List<String> list = Util.bytesToStringList(data);
		ConcurrentHashMap<String, LongAdder> wordCounts = new ConcurrentHashMap<String, LongAdder>();
		
		list.parallelStream()
			.map(line -> line.split("\\s+"))			
			.flatMap(Arrays::stream)
			.parallel()
			.filter(w -> w.matches("\\w+"))				
			.map(String::toLowerCase)
				.forEach(word -> {
					if (!wordCounts.containsKey(word))
						wordCounts.put(word, new LongAdder());
					wordCounts.get(word).increment();
				});
		
		return wordCounts;
	}

}
