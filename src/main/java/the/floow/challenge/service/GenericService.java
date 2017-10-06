package the.floow.challenge.service;

import java.util.List;

import org.bson.types.ObjectId;

import the.floow.challenge.dao.ExecutorDao;
import the.floow.challenge.dao.FileBlockDao;
import the.floow.challenge.dao.FileDao;
import the.floow.challenge.dao.MongoMessageQueue;
import the.floow.challenge.dao.SettingsDao;
import the.floow.challenge.dao.WordsDao;
import the.floow.challenge.entity.DataSource;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.enums.BlockStatus;
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
	}

	public List<Integer> getFileAvailableBlocks(ObjectId fileID) {
		return this.blockDao.getFileAvailableBlocks(fileID);
	}

	public void updateBlockStatus(List<Integer> blockNos, BlockStatus status) {
		this.blockDao.updateBlockStatus(this.fileID, blockNos, status);
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
}
