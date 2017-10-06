package the.floow.challenge.Executors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import the.floow.challenge.dao.ExecutorDao;
import the.floow.challenge.dao.FileDao;
import the.floow.challenge.dao.MongoMessageQueue;
import the.floow.challenge.dao.SettingsDao;
import the.floow.challenge.dao.WordsDao;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.entity.QueueMessage;
import the.floow.challenge.enums.BlockStatus;
import the.floow.challenge.service.WorkerService;
import the.floow.challenge.utils.Util;

public class Worker implements Runnable {

	public InputParameter inputParams;
	public ObjectId executorID;
	public ObjectId fileID;
	public WorkerService workerService;

	public ExecutorDao executorDao;
	public FileDao fileDao;
	public SettingsDao SettingsDao;
	public WordsDao wordsDao;
	ConcurrentHashMap<String, LongAdder> wordCounts;
	final static Logger logger = Logger.getLogger(Worker.class);

	public Worker(InputParameter inputParams, ObjectId executorID, ObjectId fileID) {
		this.inputParams = inputParams;
		this.executorID = executorID;
		this.fileID = fileID;
		this.workerService = new WorkerService(inputParams, executorID, fileID);

	}

	public void countWords(byte[] data) throws IOException {

		List<String> list = Util.bytesToStringList(data);

		if (this.wordCounts == null) {
			this.wordCounts = new ConcurrentHashMap<String, LongAdder>();
		}
		
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

	}

	@Override
	public void run() {

		try {
			List<Integer> processedBlocks = new ArrayList<>();
			boolean isRunning = true;
			while (isRunning) {
				MongoMessageQueue queue = this.workerService.getMessageQueue();
				if (!queue.empty()) {
					QueueMessage message = queue.dequeue();
					if (message != null) {
						this.workerService.updateBlockStatus(message.blockNo, BlockStatus.PROCESSING);
						this.countWords(message.data);
						this.workerService.updateBlockStatus(message.blockNo, BlockStatus.PROCESSED);
						processedBlocks.add(message.blockNo);
					}
				}

				isRunning = this.workerService.isBlockProcessed();
				/*
				 * write words to database if all blocks processed or memory
				 * utilization > 90% (Configurable by Setting collection)
				 */
				if (!isRunning || this.workerService.isMemoryFull()) {
					this.workerService.writeCountsToDB(this.wordCounts);
					this.workerService.updateBlockStatus(processedBlocks, BlockStatus.WRITTEN);
					this.wordCounts = null;
					processedBlocks = new ArrayList<>();
				}
			}
		} catch (Exception ex) {
			logger.error("Exception Occurs (Please see logs for more details:" + ex.getMessage());
		}
		logger.info("Worker process finished: Worker ID:"+this.executorID);
	}
}
