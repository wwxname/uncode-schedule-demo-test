package cn.uncode.schedule;

import cn.uncode.schedule.core.ScheduledDistributedMainRunnable;
import cn.uncode.schedule.core.ScheduledDistributedSubRunnable;
import cn.uncode.schedule.core.ScheduledMethodRunnable;
import cn.uncode.schedule.core.TaskDefine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;


public class DynamicTaskManager {
	
	private static final transient Logger LOGGER = LoggerFactory.getLogger(DynamicTaskManager.class);
	
	
	private static final Map<String, ScheduledFuture<?>> SCHEDULE_FUTURES = new ConcurrentHashMap<String, ScheduledFuture<?>>();
	private static final Map<String, TaskDefine> TASKS = new ConcurrentHashMap<String, TaskDefine>();
	
	
	public static void stopTask(String taskName){
		if(SCHEDULE_FUTURES.containsKey(taskName)){
			TaskDefine taskDefine = TASKS.get(taskName);
			SCHEDULE_FUTURES.get(taskDefine.stringKey()).cancel(true);
			SCHEDULE_FUTURES.remove(taskDefine.stringKey());
			TASKS.remove(taskName);
		}
	}
	/**
	 * 启动单节点定时任务
	 * @param taskDefine 任务定义
	 * @param currentTime 任务开始时间
	 */
	public static void scheduleSingleTask(TaskDefine taskDefine, Date currentTime){
		boolean newTask = true;
		if(SCHEDULE_FUTURES.containsKey(taskDefine.stringKey())){
			if(taskDefine.equals(TASKS.get(taskDefine.stringKey()))){
				newTask = false;
			}else{
				SCHEDULE_FUTURES.get(taskDefine.stringKey()).cancel(true);
				SCHEDULE_FUTURES.remove(taskDefine.stringKey());
			}
		}
		if(newTask){
			TASKS.put(taskDefine.stringKey(), taskDefine);
			if(TaskDefine.TYPE_UNCODE_SINGLE_TASK.equals(taskDefine.getType())){
				scheduleSingleTask(taskDefine);
			}
		}
	}
	
	/**
	 * 启动子定时任务
	 * @param taskDefine 任务定义
	 * @param currentTime 任务开始时间
	 */
	public static void scheduleMultiSubTask(TaskDefine taskDefine, Date currentTime){
		boolean newTask = true;
		if(SCHEDULE_FUTURES.containsKey(taskDefine.stringKey())){
			if(taskDefine.equals(TASKS.get(taskDefine.stringKey()))){
				newTask = false;
			}else{
				SCHEDULE_FUTURES.get(taskDefine.stringKey()).cancel(true);
				SCHEDULE_FUTURES.remove(taskDefine.stringKey());
			}
		}
		if(newTask){
			TASKS.put(taskDefine.stringKey(), taskDefine);
			if(TaskDefine.TYPE_UNCODE_MULTI_SUB_TASK.equals(taskDefine.getType())){
				scheduleMutilSubTask(taskDefine);
			}
		}
	}
	
	/**
	 * 启动分布式定时任务
	 * @param taskDefine 任务定义
	 * @param currentTime 任务开始时间
	 */
	public static void scheduleMultiMainTask(TaskDefine taskDefine, Date currentTime){
		boolean newTask = true;
		String scheduleKey = taskDefine.stringKey();
		if(SCHEDULE_FUTURES.containsKey(scheduleKey)){
			if(taskDefine.equals(TASKS.get(scheduleKey))){
				newTask = false;
			}else{
				SCHEDULE_FUTURES.get(scheduleKey).cancel(true);
				SCHEDULE_FUTURES.remove(scheduleKey);
			}
		}
		if(newTask){
			TASKS.put(scheduleKey, taskDefine);
			if(TaskDefine.TYPE_UNCODE_MULTI_MAIN_TASK.equals(taskDefine.getType())){
				ScheduledDistributedMainRunnable scheduledDistributedMainRunnable = new ScheduledDistributedMainRunnable(taskDefine);
				try {
					if (!SCHEDULE_FUTURES.containsKey(scheduleKey)) {
						if(taskDefine.getRunTimes() < 1){
							ScheduledFuture<?> scheduledFuture = ConsoleManager.getScheduleManager().schedule(scheduledDistributedMainRunnable, currentTime);
							if(null != scheduledFuture){
								SCHEDULE_FUTURES.put(scheduleKey, scheduledFuture);
								LOGGER.debug("Building new distribute schedule main task, target bean "+ taskDefine.getTargetBean() + " target method " + taskDefine.getTargetMethod() + ".");
							}
						}
					}
				} catch (Exception e) {
					LOGGER.error("Create distribute schedule main task error", e);
				}
			}
		}
	}
	
	public static void clearLocalTask(List<String> existsTaskName){
		for(String name:SCHEDULE_FUTURES.keySet()){
			if(!existsTaskName.contains(name)){
				SCHEDULE_FUTURES.get(name).cancel(true);
				SCHEDULE_FUTURES.remove(name);
				TASKS.remove(name);
			}
		}
	}
	/**
	 * 启动子定时任务
	 * 支持：
	 * 1 startTime，指定时间执行
	 * 
	 * @param taskDefine 任务定义
	 */
	public static void scheduleMutilSubTask(TaskDefine taskDefine){
		String scheduleKey = taskDefine.stringKey();
		try {
			if (!SCHEDULE_FUTURES.containsKey(scheduleKey)) {
				ScheduledFuture<?> scheduledFuture = null;
				ScheduledDistributedSubRunnable scheduledMethodRunnable = null;
				try {
					scheduledMethodRunnable = buildScheduledSubRunnable(taskDefine);
				} catch (Exception e) {
					try {
						ConsoleManager.getScheduleManager().getScheduleDataManager().saveRunningInfo(scheduleKey, ConsoleManager.getScheduleManager().getScheduleServerUUid(), taskDefine.getRunTimes(), "method is null");
					} catch (Exception e1) {
						LOGGER.debug(e.getLocalizedMessage(), e);
					}
					LOGGER.debug(e.getLocalizedMessage(), e);
				}
				if(scheduledMethodRunnable != null){
					if(null != taskDefine.getStartTime()){
						scheduledFuture = ConsoleManager.getScheduleManager().scheduleWithFixedDelay(scheduledMethodRunnable, taskDefine.getStartTime(), 300);
					}
				}
				if(null != scheduledFuture){
					SCHEDULE_FUTURES.put(scheduleKey, scheduledFuture);
					LOGGER.debug("Building new sub schedule task, target bean "+ taskDefine.getTargetBean() + " target method " + taskDefine.getTargetMethod() + ".");
				}
			}else{
				ConsoleManager.getScheduleManager().getScheduleDataManager()
				.saveRunningInfo(scheduleKey, ConsoleManager.getScheduleManager().getScheduleServerUUid(), taskDefine.getRunTimes(), "bean not exists");
				LOGGER.debug("Bean name is not exists.");
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	
	/**
	 * 启动定时任务
	 * 支持：
	 * 1 cron时间表达式，立即执行
	 * 2 startTime + period,指定时间，定时进行
	 * 3 period，定时进行，立即开始
	 * 4 startTime，指定时间执行
	 * 
	 * @param taskDefine 任务定义
	 */
	public static void scheduleSingleTask(TaskDefine taskDefine){
		String scheduleKey = taskDefine.stringKey();
		try {
			if (!SCHEDULE_FUTURES.containsKey(scheduleKey)) {
				ScheduledFuture<?> scheduledFuture = null;
				ScheduledMethodRunnable scheduledMethodRunnable = buildScheduledRunnable(taskDefine);
				if(scheduledMethodRunnable != null){
					if(StringUtils.isNotEmpty(taskDefine.getCronExpression())){
						Trigger trigger = new CronTrigger(taskDefine.getCronExpression());
						scheduledFuture = ConsoleManager.getScheduleManager().schedule(scheduledMethodRunnable, trigger);
					}else{
						if(null != taskDefine.getStartTime()){
							if(taskDefine.getPeriod() > 0){
								scheduledFuture = ConsoleManager.getScheduleManager().scheduleAtFixedRate(scheduledMethodRunnable, taskDefine.getStartTime(), taskDefine.getPeriod());
							}else if(taskDefine.getDelay() > 0){
								scheduledFuture = ConsoleManager.getScheduleManager().scheduleWithFixedDelay(scheduledMethodRunnable, taskDefine.getStartTime(), taskDefine.getDelay());
							}else{
								if(taskDefine.getRunTimes() < 1){//防止单次任务节点重启后重复执行
									scheduledFuture = ConsoleManager.getScheduleManager().schedule(scheduledMethodRunnable, taskDefine.getStartTime());
								}
							}
						}else{
							if(taskDefine.getPeriod() > 0){
								scheduledFuture = ConsoleManager.getScheduleManager().scheduleAtFixedRate(scheduledMethodRunnable, taskDefine.getPeriod());
							}else if(taskDefine.getDelay() > 0){
								scheduledFuture = ConsoleManager.getScheduleManager().scheduleWithFixedDelay(scheduledMethodRunnable, taskDefine.getDelay());
							}
						}
					}
						
					if(null != scheduledFuture){
						SCHEDULE_FUTURES.put(scheduleKey, scheduledFuture);
						LOGGER.debug("Building new schedule task, target bean "+ taskDefine.getTargetBean() + " target method " + taskDefine.getTargetMethod() + ".");
					}
				}else{
					ConsoleManager.getScheduleManager().getScheduleDataManager()
					.saveRunningInfo(scheduleKey, ConsoleManager.getScheduleManager().getScheduleServerUUid(), taskDefine.getRunTimes(), "bean not exists");
					LOGGER.debug("Bean name is not exists.");
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	/**
	 * 封装任务对象
	 * @param taskDefine 任务定义
	 * @return 分布式子任务
	 */
	private static ScheduledDistributedSubRunnable buildScheduledSubRunnable(TaskDefine taskDefine){
		Object bean;
		ScheduledDistributedSubRunnable scheduledMethodRunnable = null;
		try {
			bean = ZKScheduleManager.getApplicationcontext().getBean(taskDefine.getTargetBean());
			Method method = getMethod(bean, taskDefine.getTargetMethod(), String.class);
			scheduledMethodRunnable = new ScheduledDistributedSubRunnable(taskDefine);
			if(method == null){
				throw new Exception("执行方法为空或没有String类型参数");
			}
		} catch (Exception e) {
			try {
				ConsoleManager.getScheduleManager().getScheduleDataManager().saveRunningInfo(taskDefine.stringKey(), ConsoleManager.getScheduleManager().getScheduleServerUUid(), taskDefine.getRunTimes(), "method is null");
			} catch (Exception e1) {
				LOGGER.debug(e.getLocalizedMessage(), e);
			}
			LOGGER.debug(e.getLocalizedMessage(), e);
		}
		return scheduledMethodRunnable;
	}
	
	/**
	 * 封装任务对象
	 * @param taskDefine 任务定义
	 * @return 单任务
	 */
	private static ScheduledMethodRunnable buildScheduledRunnable(TaskDefine taskDefine){
		ScheduledMethodRunnable scheduledMethodRunnable = null;
		try {
			scheduledMethodRunnable = new ScheduledMethodRunnable(taskDefine);
		} catch (Exception e) {
			try {
				ConsoleManager.getScheduleManager().getScheduleDataManager().saveRunningInfo(taskDefine.stringKey(), ConsoleManager.getScheduleManager().getScheduleServerUUid(), taskDefine.getRunTimes(),"method is null");
			} catch (Exception e1) {
				LOGGER.debug(e.getLocalizedMessage(), e);
			}
			LOGGER.debug(e.getLocalizedMessage(), e);
		}
		return scheduledMethodRunnable;
	}
	
	public static Method getMethod(Object bean, String targetMethod, Class<?>... paramTypes){
		
		Method method = null;
		Class<?> clazz;
		if (AopUtils.isAopProxy(bean)) {
			clazz = AopProxyUtils.ultimateTargetClass(bean);
		} else {
			clazz = bean.getClass();
		}
		if(null != paramTypes){
			method = ReflectionUtils.findMethod(clazz, targetMethod, paramTypes);
		}else{
			method = ReflectionUtils.findMethod(clazz, targetMethod);
		}
		return method;
	}
}
