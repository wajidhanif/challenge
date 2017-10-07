package the.floow.challenge.service;

import the.floow.challenge.dao.SettingsDao;
import the.floow.challenge.entity.InputParameter;

public class ChallengeService{
	
	public SettingsDao settingDao;
	public ChallengeService(InputParameter inParam) {
		this.settingDao = new SettingsDao(inParam.dataSouce);
	}
	
	public void addSetting(String name, String value){
		this.settingDao.create(name, value);
	}
}
