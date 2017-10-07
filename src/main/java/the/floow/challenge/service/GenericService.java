package the.floow.challenge.service;

import static com.mongodb.client.model.Filters.eq;

import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;

import the.floow.challenge.dao.ExecutorDao;
import the.floow.challenge.dao.FileBlockDao;
import the.floow.challenge.dao.FileDao;
import the.floow.challenge.dao.MongoMessageQueue;
import the.floow.challenge.dao.SettingsDao;
import the.floow.challenge.dao.WordsDao;
import the.floow.challenge.entity.DataSource;
import the.floow.challenge.entity.Executor;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.enums.BlockStatus;
import the.floow.challenge.enums.ExecutorStatus;
import the.floow.challenge.utils.Util;


public class GenericService {
	
	public InputParameter inputParams;
	public ObjectId executorID;
	public ObjectId fileID;
	public ExecutorDao executorDao;
	public FileDao fileDao;
	public SettingsDao SettingsDao;
	public WordsDao wordsDao;
	public FileBlockDao blockDao;
	public long blockSize;
	public long DefaultQueueSizeMultipler;
	public double DefaultMemoryUsedPercentage;
	public long workerMaxWaitTime;
	
	public GenericService(InputParameter inputParams, ObjectId executorID, ObjectId fileID){
		this.inputParams = inputParams;
		this.executorID = executorID;
		this.fileID = fileID;
		DataSource ds = this.inputParams.dataSouce;
		this.executorDao = new ExecutorDao(ds);
		this.fileDao = new FileDao(ds);
		this.SettingsDao = new SettingsDao(ds);
		this.wordsDao = new WordsDao(ds);
		this.blockDao = new FileBlockDao(ds);
		this.blockSize = Long.parseLong(this.SettingsDao.getSetting("BLOCK_SIZE"));
		this.DefaultQueueSizeMultipler = Long.parseLong(this.SettingsDao.getSetting("DEFAULT_QUEUE_MULTIPLER"));
		this.DefaultMemoryUsedPercentage = Double.parseDouble(this.SettingsDao.getSetting("DEFAULT_MEMORY_USED_PERCENTAGE"));
		this.workerMaxWaitTime = Long.parseLong(this.SettingsDao.getSetting("WORKER_MAX_WAIT_TIME"));
	}

	public List<Integer> getFileAvailableBlocks(ObjectId fileID) {
		return this.blockDao.getFileAvailableBlocks(fileID);
	}

	public void updateBlockStatus(List<Integer> blockNos, BlockStatus status) {
		this.blockDao.updateBlockStatus(this.fileID, blockNos, status);
	}
	
	public long getBlockCountByStatus(BlockStatus status) {
		return this.blockDao.getBlockCountByStatus(this.fileID, status);
	}
	public boolean isProcessFinished() {
		long notWrittenCount = this.blockDao.getBlockCountNotWritten(this.fileID);
		return notWrittenCount > 0 ? true : false;
	}
	public long getExecutorCount() {
		return this.executorDao.getExecutorCount();
	}

	public MongoMessageQueue getMessageQueue() {
		return new MongoMessageQueue(this.inputParams.dataSouce);
	}
	
	public void updateBlockStatus(int blockNo, BlockStatus status) {
		this.blockDao.updateBlockStatus(this.fileID,this.executorID, blockNo, status);
	}
	
	public boolean isMemoryFull() {
		return Util.getMemoryUsePercentage() > this.DefaultMemoryUsedPercentage ? true : false;
	}
	
	public void updateExectorStatus(ExecutorStatus status){
		this.executorDao.updateExectorStatus(this.executorID, status);
	}
	public Executor getExecutor(){
		return this.executorDao.getExecutor(this.executorID);
	}
	public void updateExectorRunningTime(){
		this.executorDao.updateExectorRunningTime(this.executorID);
	}
	
}
