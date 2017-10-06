package the.floow.challenge.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.bson.types.ObjectId;

import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.enums.BlockStatus;

public class WorkerService extends GenericService {

	public WorkerService(InputParameter inputParams, ObjectId executorID, ObjectId fileID) {
		super(inputParams, executorID, fileID);
	}

	public boolean isBlockProcessed() {
		long processedCount = this.blockDao.getBlockCountByStatus(this.fileID, BlockStatus.PROCESSED);
		long totalCount = this.blockDao.getAllBlockCount(this.fileID);
		return processedCount != totalCount ? true : false;
	}

	public void writeCountsToDB(ConcurrentHashMap<String, LongAdder> words) {
		this.wordsDao.create(this.fileID, this.executorID, words);
	}

}
