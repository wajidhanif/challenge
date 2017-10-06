package the.floow.challenge.service;

import java.io.IOException;
import java.net.UnknownHostException;

import org.bson.types.ObjectId;

import the.floow.challenge.dao.SettingsDao;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.enums.ExecutorStatus;
import the.floow.challenge.utils.FileUtil;
import the.floow.challenge.utils.Util;

public class ChallengeService{
	
	public SettingsDao settingDao;
	public ChallengeService(InputParameter inParam) {
		this.settingDao = new SettingsDao(inParam.dataSouce);
	}
	
	public void addSetting(String name, String value){
		this.settingDao.create(name, value);
	}
}
