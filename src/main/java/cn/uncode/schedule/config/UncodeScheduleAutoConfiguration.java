package cn.uncode.schedule.config;

import cn.uncode.schedule.ZKScheduleManager;
import cn.uncode.schedule.core.TaskDefine;
import cn.uncode.schedule.util.ScheduleUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by KevinBlandy on 2017/2/28 14:11
 */
@Configuration
@EnableConfigurationProperties({UncodeScheduleConfig.class})
@EnableScheduling
@ComponentScan({"cn.uncode.schedule"})
@ServletComponentScan({"cn.uncode.schedule"})
public class UncodeScheduleAutoConfiguration {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(UncodeScheduleAutoConfiguration.class);
	
	@Autowired
	private UncodeScheduleConfig uncodeScheduleConfig;
	
	@Bean(name = "zkScheduleManager", initMethod="init")
	public ZKScheduleManager commonMapper(){
		ZKScheduleManager zkScheduleManager = new ZKScheduleManager();
		zkScheduleManager.setZkConfig(uncodeScheduleConfig.getConfig());
		List<TaskDefine> list = initAllTask();
		zkScheduleManager.setInitTaskDefines(list);
		LOGGER.info("=====>ZKScheduleManager inited..");
		return zkScheduleManager;
	}
	
	private List<TaskDefine> initAllTask(){
		List<TaskDefine> list = new ArrayList<TaskDefine>();
		int total = 0;
		if(uncodeScheduleConfig.getTargetBean() != null){
			total = uncodeScheduleConfig.getTargetBean().size();
		}
		for(int i = 0; i < total; i++){
			TaskDefine taskDefine = new TaskDefine();
			if(uncodeScheduleConfig.getTargetBean() != null){
				 String value = uncodeScheduleConfig.getTargetBean().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setTargetBean(value);
				}
			}
			if(uncodeScheduleConfig.getTargetMethod() != null){
				 String value = uncodeScheduleConfig.getTargetMethod().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setTargetMethod(value);
				}
			}
			if(uncodeScheduleConfig.getCronExpression() != null){
				 String value = uncodeScheduleConfig.getCronExpression().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setCronExpression(value);
				}
			}
			if(uncodeScheduleConfig.getStartTime() != null){
				 String value = uncodeScheduleConfig.getStartTime().get(i);
				if(StringUtils.isNotBlank(value)){
					Date time = null;
					try {
						time = ScheduleUtil.transferStringToDate(value);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					if(time != null){
						taskDefine.setStartTime(time);
					}
				}
			}
			if(uncodeScheduleConfig.getPeriod() != null){
				 String value = uncodeScheduleConfig.getPeriod().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setPeriod(Long.valueOf(value));
				}
			}
			if(uncodeScheduleConfig.getDelay() != null){
				 String value = uncodeScheduleConfig.getDelay().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setDelay(Long.valueOf(value));
				}
			}
			
			if(uncodeScheduleConfig.getParams() != null){
				 String value = uncodeScheduleConfig.getParams().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setParams(value);
				}
			}
			
			if(uncodeScheduleConfig.getType() != null){
				 String value = uncodeScheduleConfig.getType().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setType(value);
				}
			}
			
			if(uncodeScheduleConfig.getExtKeySuffix() != null){
				 String value = uncodeScheduleConfig.getExtKeySuffix().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setExtKeySuffix(value);
				}
			}
			if(uncodeScheduleConfig.getBeforeMethod() != null){
				 String value = uncodeScheduleConfig.getBeforeMethod().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setBeforeMethod(value);
				}
			}
			if(uncodeScheduleConfig.getAfterMethod() != null){
				 String value = uncodeScheduleConfig.getAfterMethod().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setAfterMethod(value);
				}
			}
			if(uncodeScheduleConfig.getThreadNum() != null){
				 String value = uncodeScheduleConfig.getThreadNum().get(i);
				if(StringUtils.isNotBlank(value)){
					taskDefine.setThreadNum(Integer.valueOf(value));
				}
			}
			list.add(taskDefine);
		}
		return list;
	}
	
	
	
}
