package the.floow.challenge.processor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import the.floow.challenge.utils.Util;

public class WordCountProcessor implements CountProcessor {

	@Override
	public ConcurrentHashMap<String, LongAdder> counts(byte[] data) throws IOException {
		List<String> list = Util.bytesToStringList(data);
		ConcurrentHashMap<String, LongAdder> wordCounts = new ConcurrentHashMap<String, LongAdder>();
		
		list.parallelStream()
			.map(line -> line.split("\\s+"))			/* split line into words*/
			.flatMap(Arrays::stream)
			.parallel()
			.filter(w -> w.matches("\\w+"))				/*filter out non-word items*/
			.map(String::toLowerCase)
				.forEach(word -> {
					if (!wordCounts.containsKey(word))
						wordCounts.put(word, new LongAdder());
					wordCounts.get(word).increment();
				});
		
		return wordCounts;
	}

}
