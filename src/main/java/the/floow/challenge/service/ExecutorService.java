package the.floow.challenge.service;

import java.io.IOException;
import java.net.UnknownHostException;

import org.bson.types.ObjectId;

import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.enums.ExecutorStatus;
import the.floow.challenge.utils.FileUtil;
import the.floow.challenge.utils.Util;

public class ExecutorService extends GenericService{
	
	public boolean isFileExist;
	
	public ExecutorService(InputParameter inParams) {
		super(inParams, null, null);
		this.isFileExist = true;
	}
	
	public ObjectId createFile(ObjectId executorID) throws IOException{
		ObjectId fileID=null;		
		if (this.inputParams != null && this.inputParams.fileName.length() > 0) {
			
			if(!this.fileDao.isFileExist(this.inputParams.fileName,executorID)){
				this.fileDao.create(this.inputParams.fileName, this.inputParams.filePath, executorID);
			}
			fileID = this.fileDao.getFileID();
			if (fileID == null) {			
				fileID = this.fileDao.setFileStatusToProcessing(this.inputParams.fileName);
				long blockCounts = this.blockDao.getAllBlockCount(fileID);
				if(blockCounts == 0){
					this.blockDao.createFileBlocks(fileID, this.blockSize, FileUtil.getFileSize(this.inputParams.filePath));
				}
			}
		}else{
			//if no file parameter, then get the first file which is processing.  
			fileID = this.fileDao.getFileID();
			this.isFileExist = false;
		}
		this.fileID = fileID;
		this.executorID = executorID;
		return fileID;
	}	

	public ObjectId register() throws UnknownHostException {
		String hostName = Util.getHostName();
		ObjectId executorID = this.executorDao.getExecutor(hostName);
		if(executorID==null){
			executorID = this.executorDao.create(hostName);
		}else{
			this.executorDao.updateExectorStatus(executorID, ExecutorStatus.LIVE);
		}
		return executorID;
	}
	/*
	public boolean isExecutorAsServer(){
		/* if executor does not have file, then it cannot be a server
		if(!this.isFileExist){
			return false;
		}			
		ObjectId exeID = this.executorDao.getExecutorAsServer();
		if(exeID== null){
			exeID = this.executorDao.updateExecutorAsServer();
		}
		return this.executorID == exeID ? true : false;
	}
*/
	public boolean isExecutorAsController(){
		return this.isFileExist;
	}
}
