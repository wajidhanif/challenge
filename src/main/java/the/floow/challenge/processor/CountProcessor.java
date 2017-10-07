package the.floow.challenge.processor;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public interface CountProcessor {
	
	ConcurrentHashMap<String, LongAdder> counts(byte [] data) throws IOException;

}
