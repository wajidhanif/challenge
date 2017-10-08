package the.floow.challenge.entity;

import java.util.Date;

import org.bson.types.ObjectId;

import the.floow.challenge.enums.ExecutorStatus;

public class Executor {

	public ObjectId id;
	public String name;
    public ExecutorStatus status;
    public Date runningTimestamp;
    
	public Executor(ObjectId id, String name, ExecutorStatus status) {
		this.id = id;
		this.name = name;
		this.status = status;
	}
	public Executor(ObjectId id, String name, String status, Date runTime) {
		this.id = id;
		this.name = name;
		this.status = ExecutorStatus.valueOf(status.toUpperCase());
		this.runningTimestamp= runTime;
	}
	public Executor(String name, String status) {
		this.name = name;
		this.status = ExecutorStatus.valueOf(status);
	}
   
}
