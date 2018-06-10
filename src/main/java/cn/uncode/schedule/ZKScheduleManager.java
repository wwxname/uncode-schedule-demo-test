package cn.uncode.schedule;

import cn.uncode.schedule.core.*;
import cn.uncode.schedule.zk.ScheduleDataManager4ZK;
import cn.uncode.schedule.zk.ZKManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 调度器核心管理
 * 
 * @author juny.ye
 * 
 */
public class ZKScheduleManager extends ThreadPoolTaskScheduler implements ApplicationContextAware {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final int DEFAULT_POOL_SIZE = 20;

	private static final transient Logger LOGGER = LoggerFactory.getLogger(ZKScheduleManager.class);
	
	private final CountDownLatch downLatch = new CountDownLatch(1);

	private Map<String, String> zkConfig;
	
	protected ZKManager zkManager;

	private IScheduleDataManager scheduleDataManager;

	/**
	 * 当前调度服务的信息
	 */
	protected ScheduleServer currenScheduleServer;

	/**
	 * 是否启动调度管理，如果只是做系统管理，应该设置为false,对应key值为onlyAdmin
	 */
	public boolean start = true;

	/**
	 * 心跳间隔
	 */
	private int timerInterval = 1000;

	/**
	 * 是否注册成功
	 */
	private boolean isScheduleServerRegister = true;

	private static ApplicationContext applicationcontext;
	
	private Map<String, Boolean> isOwnerMap = new ConcurrentHashMap<String, Boolean>();

	private Timer hearBeatTimer;
	private Lock initLock = new ReentrantLock();
	private boolean isStopSchedule = false;
	private Lock registerLock = new ReentrantLock();
	
	private List<TaskDefine> initTaskDefines = new ArrayList<TaskDefine>();
	
	private volatile String errorMessage = "No config Zookeeper connect information";
	private InitialThread initialThread;

	public ZKScheduleManager() {
		this.currenScheduleServer = ScheduleServer.createScheduleServer(null);
	}

	public void init() throws Exception {
		if(this.zkConfig != null){
			for (Map.Entry<String, String> e : this.zkConfig.entrySet()) {
				ConsoleManager.properties.put(e.getKey(), e.getValue());
			}
		}
		if(ConsoleManager.properties.containsKey("onlyClient")){
			String val = String.valueOf(ConsoleManager.properties.get("onlyClient"));
			if(StringUtils.isNotBlank(val)){
				start = Boolean.valueOf(val);
			}
		}
		this.setPoolSize(DEFAULT_POOL_SIZE);
		if(ConsoleManager.properties.containsKey("poolSize")){
			String val = String.valueOf(ConsoleManager.properties.get("poolSize"));
			if(StringUtils.isNotBlank(val)){
				this.setPoolSize(Integer.valueOf(val));
			}
		}
		System.out.println("properties:"+ConsoleManager.properties);
		this.init(ConsoleManager.properties);
	}

	public void init(Properties p) throws Exception {
		if (this.initialThread != null) {
			this.initialThread.stopThread();
		}
		this.initLock.lock();
		try {
			this.scheduleDataManager = null;
			if (this.zkManager != null) {
				this.zkManager.close();
			}
			this.zkManager = new ZKManager(p);
			this.errorMessage = "Zookeeper connecting ......"
					+ this.zkManager.getConnectStr();
			initialThread = new InitialThread(this);
			initialThread.setName("ScheduleManager-initialThread");
			initialThread.start();
		} finally {
			this.initLock.unlock();
		}
	}

	private void rewriteScheduleInfo() throws Exception {
		registerLock.lock();
		try {
			if (this.isStopSchedule) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("外部命令终止调度,不在注册调度服务，避免遗留垃圾数据："
							+ currenScheduleServer.getUuid());
				}
				return;
			}
			// 先发送心跳信息
			if (errorMessage != null) {
				this.currenScheduleServer.setDealInfoDesc(errorMessage);
			}
			if (!this.scheduleDataManager
					.refreshScheduleServer(this.currenScheduleServer)) {
				// 更新信息失败，清除内存数据后重新注册
				this.clearMemoInfo();
				this.scheduleDataManager.registerScheduleServer(this.currenScheduleServer);
			}
			isScheduleServerRegister = true;
		} finally {
			registerLock.unlock();
		}
	}

	/**
	 * 清除内存中所有的已经取得的数据和任务队列,在心态更新失败，或者发现注册中心的调度信息被删除
	 */
	public void clearMemoInfo() {
		try {

		} finally {
		}

	}

	/**
	 * 根据当前调度服务器的信息，重新计算分配所有的调度任务
	 * 任务的分配是需要加锁，避免数据分配错误。为了避免数据锁带来的负面作用，通过版本号来达到锁的目的
	 * 
	 * 1、获取任务状态的版本号 2、获取所有的服务器注册信息和任务队列信息 3、清除已经超过心跳周期的服务器注册信息 3、重新计算任务分配
	 * 4、更新任务状态的版本号【乐观锁】 5、根系任务队列的分配信息
	 * 
	 * @throws Exception
	 */
	public void assignScheduleTask() throws Exception {
		scheduleDataManager.clearExpireScheduleServer();
		List<String> serverList = scheduleDataManager.loadScheduleServerNames();
		if (!scheduleDataManager.isLeader(this.currenScheduleServer.getUuid(),
				serverList)) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(this.currenScheduleServer.getUuid()
						+ ":不是负责任务分配的Leader,直接返回");
			}
			return;
		}
		//黑名单
		for(String ip:zkManager.getIpBlacklist()){
			int index = serverList.indexOf(ip);
			if (index > -1){
				serverList.remove(index);
			}
		}
		// 设置初始化成功标准，避免在leader转换的时候，新增的线程组初始化失败
		scheduleDataManager.assignTask(this.currenScheduleServer.getUuid(), serverList);
	}

	/**
	 * 定时向数据配置中心更新当前服务器的心跳信息。 如果发现本次更新的时间如果已经超过了，服务器死亡的心跳周期，则不能在向服务器更新信息。
	 * 而应该当作新的服务器，进行重新注册。
	 * 
	 * @throws Exception
	 */
	public void refreshScheduleServer() throws Exception {
		try {
			rewriteScheduleInfo();
			// 如果任务信息没有初始化成功，不做任务相关的处理
			if (!this.isScheduleServerRegister) {
				return;
			}

			// 重新分配任务
			this.assignScheduleTask();
			// 检查本地任务
			this.checkLocalTask();
		} catch (Throwable e) {
			// 清除内存中所有的已经取得的数据和任务队列,避免心跳线程失败时候导致的数据重复
			this.clearMemoInfo();
			if (e instanceof Exception) {
				throw (Exception) e;
			} else {
				throw new Exception(e.getMessage(), e);
			}
		}
	}
	
	public void checkLocalTask() throws Exception {
		// 检查系统任务执行情况
		scheduleDataManager.checkLocalTask(this.currenScheduleServer.getUuid());
	}

	/**
	 * 在Zk状态正常后回调数据初始化
	 * 
	 * @throws Exception
	 */
	public void initialData() throws Exception {
		this.zkManager.initial();
		this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
		checkScheduleDataManager();
		if (this.start) {
			// 注册调度管理器
			this.scheduleDataManager.registerScheduleServer(this.currenScheduleServer);
			if (hearBeatTimer == null) {
				hearBeatTimer = new Timer("ScheduleManager-"
						+ this.currenScheduleServer.getUuid() + "-HearBeat");
			}
			hearBeatTimer.schedule(new HeartBeatTimerTask(this), 1000, this.timerInterval);
			
			//初始化启动数据
			if(initTaskDefines != null && initTaskDefines.size() > 0){
				for(TaskDefine taskDefine : initTaskDefines){
					scheduleDataManager.addTask(taskDefine);
				}
			}
		}
	}
	
	private Runnable taskWrapper(final Runnable task){
		return new Runnable(){
			public void run(){
				TaskDefine taskDefine = resolveTaskName(task);
				String name = taskDefine.stringKey();
		    	if(StringUtils.isNotEmpty(name)){
		    		boolean isOwner = false;
		    		boolean isRunning = true;
					try {
						if(!isScheduleServerRegister){
							Thread.sleep(1000);
						}
						if(zkManager.checkZookeeperState()){
							isOwner = scheduleDataManager.isOwner(name, currenScheduleServer.getUuid());
							isOwnerMap.put(name, isOwner);
							isRunning = scheduleDataManager.isRunning(name);
						}else{
							// 如果zk不可用，使用历史数据
							if(null != isOwnerMap){
								isOwner = isOwnerMap.get(name);
							}
						}
						if(isOwner && isRunning){
							String msg = null;
			    			try {
			    				task.run();
				    			LOGGER.info("Cron job has been executed.");
							} catch (Exception e) {
								msg = e.getLocalizedMessage();
							}
			    			scheduleDataManager.saveRunningInfo(name, currenScheduleServer.getUuid(), taskDefine.getRunTimes(), msg);
			    		}
					} catch (Exception e) {
						LOGGER.error("Check task owner error.", e);
					}
		    	}
			}
		};
	}
	
	private TaskDefine resolveTaskName(final Runnable task) {
		Method targetMethod = null;
		TaskDefine taskDefine = new TaskDefine();
		if(task instanceof ScheduledMethodRunnable){
			ScheduledMethodRunnable runnable = (ScheduledMethodRunnable)task;
			taskDefine.setType(TaskDefine.TYPE_UNCODE_SINGLE_TASK);
			taskDefine.valueOf(runnable.getTaskDefine());
			taskDefine.setRunTimes(runnable.getRunTimes());
		}else if(task instanceof ScheduledDistributedSubRunnable){
			ScheduledDistributedSubRunnable runnable = (ScheduledDistributedSubRunnable)task;
			taskDefine.setType(TaskDefine.TYPE_UNCODE_MULTI_SUB_TASK);
			taskDefine.valueOf(runnable.getTaskDefine());
			taskDefine.setRunTimes(runnable.getRunTimes());
		}else if(task instanceof ScheduledDistributedMainRunnable){
			ScheduledDistributedMainRunnable runnable = (ScheduledDistributedMainRunnable)task;
			taskDefine.valueOf(runnable.getTaskDefine());
			taskDefine.setRunTimes(runnable.getRunTimes());
			taskDefine.setType(TaskDefine.TYPE_UNCODE_MULTI_MAIN_TASK);
		}else{
			org.springframework.scheduling.support.ScheduledMethodRunnable springScheduledMethodRunnable = (org.springframework.scheduling.support.ScheduledMethodRunnable)task;
			targetMethod = springScheduledMethodRunnable.getMethod();
			taskDefine.setType(TaskDefine.TYPE_SPRING_TASK);
			String[] beanNames = applicationcontext.getBeanNamesForType(targetMethod.getDeclaringClass());
	    	if(null != beanNames && StringUtils.isNotEmpty(beanNames[0])){
	    		taskDefine.setTargetBean(beanNames[0]);
	    		taskDefine.setTargetMethod(targetMethod.getName());
	    	}
		}
		
		return taskDefine;
	}

	class HeartBeatTimerTask extends java.util.TimerTask {
		private transient final Logger log = LoggerFactory.getLogger(HeartBeatTimerTask.class);
		ZKScheduleManager manager;

		public HeartBeatTimerTask(ZKScheduleManager aManager) {
			manager = aManager;
		}

		public void run() {
			try {
				Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
				manager.refreshScheduleServer();
			} catch (Exception ex) {
				log.error(ex.getMessage(), ex);
			}
		}
	}

	class InitialThread extends Thread {
		private transient Logger log = LoggerFactory.getLogger(InitialThread.class);
		ZKScheduleManager sm;

		public InitialThread(ZKScheduleManager sm) {
			this.sm = sm;
		}

		boolean isStop = false;

		public void stopThread() {
			this.isStop = true;
		}

		@Override
		public void run() {
			sm.initLock.lock();
			try {
				int count = 0;
				while (!sm.zkManager.checkZookeeperState()) {
					count = count + 1;
					if (count % 50 == 0) {
						sm.errorMessage = "Zookeeper connecting ......"
								+ sm.zkManager.getConnectStr() + " spendTime:"
								+ count * 20 + "(ms)";
						log.error(sm.errorMessage);
					}
					Thread.sleep(20);
					if (this.isStop) {
						return;
					}
				}
				sm.initialData();
			} catch (Throwable e) {
				log.error(e.getMessage(), e);
			} finally {
				sm.initLock.unlock();
			}

		}

	}

	public IScheduleDataManager getScheduleDataManager() {
		return scheduleDataManager;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationcontext)
			throws BeansException {
		ZKScheduleManager.applicationcontext = applicationcontext;
	}
	
	public void setZkManager(ZKManager zkManager) {
		this.zkManager = zkManager;
	}
	
	public ZKManager getZkManager() {
		return zkManager;
	}

	public void setZkConfig(Map<String, String> zkConfig) {
		this.zkConfig = zkConfig;
	}
	
	/**
     * 使用fixedRate的方式提交任务调度请求
     * <pre>
     * 任务首次启动时间未设置，任务池将会尽可能早的启动任务 
     * </pre>
     * 
     * @param task　待执行的任务　
     * @param period　两次任务启动时间之间的间隔时间，默认单位是毫秒
     * @return 任务句柄
     */
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
		try {
			TaskDefine taskDefine = resolveTaskName(task);
			taskDefine.setPeriod(period);
			checkScheduleDataManager();
			boolean rt = isUncodeTask(task);
			if(rt == false){
				scheduleDataManager.addTask(taskDefine);
			}
			LOGGER.debug(currenScheduleServer.getUuid() +":自动向集群注册任务[" + taskDefine.stringKey() + "]");
		} catch (Exception e) {
			LOGGER.error("update task error", e);
		}
        return super.scheduleAtFixedRate(taskWrapper(task), period);
    }
	
    /**
     * 提交任务调度请求 
     * 
     * @param task　待执行任务　　
     * @param trigger 使用Trigger指定任务调度规则
     * @return 任务句柄
     */
	public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
		try {
			TaskDefine taskDefine = resolveTaskName(task);
			String cronEx = trigger.toString();
			int index = cronEx.indexOf(":");
			if(index >= 0){
				cronEx = cronEx.substring(index + 1);
				taskDefine.setCronExpression(cronEx.trim());
			}
			checkScheduleDataManager();
			boolean rt = isUncodeTask(task);
			if(rt == false){
				scheduleDataManager.addTask(taskDefine);
			}
			LOGGER.debug(currenScheduleServer.getUuid() +":自动向集群注册任务[" + taskDefine.getSingalKey() + "]");
		} catch (Exception e) {
			LOGGER.error("update task error", e);
		}
		return super.schedule(taskWrapper(task), trigger);
	}

	/**
     * 提交任务调度请求
     * <pre>
     * 注意任务只执行一次，使用startTime指定其启动时间  
     * </pre>
     * @param task　待执行任务
     * @param startTime 任务启动时间  
     * @return 任务句柄
     */
	public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
		try {
			TaskDefine taskDefine = resolveTaskName(task);
			taskDefine.setStartTime(startTime);
			checkScheduleDataManager();
			boolean rt = isUncodeTask(task);
			if(rt == false){
				scheduleDataManager.addTask(taskDefine);
			}
			LOGGER.debug(currenScheduleServer.getUuid() +":自动向集群注册任务[" + taskDefine.getSingalKey() + "]");
		} catch (Exception e) {
			LOGGER.error("update task error", e);
		}
		return super.schedule(taskWrapper(task), startTime);
	}

	private void checkScheduleDataManager() throws InterruptedException {
		if(scheduleDataManager == null){
			downLatch.await(1000, TimeUnit.MILLISECONDS);
		}else{
			downLatch.countDown();
		}
	}
	
	private boolean isUncodeTask(Runnable task){
		if(task instanceof ScheduledMethodRunnable){
			return true;
		}else if(task instanceof ScheduledDistributedSubRunnable){
			return true;
		}else if(task instanceof ScheduledDistributedMainRunnable){
			return true;
		}
		return false;
	}

	/**
     * 使用fixedRate的方式提交任务调度请求
     * <pre>
     * 任务首次启动时间由传入参数指定 
     * </pre>
     * @param task　待执行的任务　
     * @param startTime　任务启动时间  
     * @param period　两次任务启动时间之间的间隔时间，默认单位是毫秒
     * @return 任务句柄
     */
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
		try {
			TaskDefine taskDefine = resolveTaskName(task);
			taskDefine.setStartTime(startTime);
			taskDefine.setPeriod(period);
			checkScheduleDataManager();
			boolean rt = isUncodeTask(task);
			if(rt == false){
				scheduleDataManager.addTask(taskDefine);
			}
			LOGGER.debug(currenScheduleServer.getUuid() +":自动向集群注册任务[" + taskDefine.getSingalKey() + "]");
		} catch (Exception e) {
			LOGGER.error("update task error", e);
		}
		return super.scheduleAtFixedRate(taskWrapper(task), startTime, period);
	}
	

	/**
     *  使用fixedDelay的方式提交任务调度请求
     * <pre>
     *  任务首次启动时间由传入参数指定 
     * </pre>
     * @param task 待执行任务
     * @param startTime 任务启动时间
     * @param delay 上一次任务结束时间与下一次任务开始时间的间隔时间，单位默认是毫秒 
     * @return 任务句柄
     */
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
		try {
			TaskDefine taskDefine = resolveTaskName(task);
			taskDefine.setStartTime(startTime);
			taskDefine.setPeriod(delay);
			checkScheduleDataManager();
			boolean rt = isUncodeTask(task);
			if(rt == false){
				scheduleDataManager.addTask(taskDefine);
			}
			LOGGER.debug(currenScheduleServer.getUuid() +":自动向集群注册任务[" + taskDefine.getSingalKey() + "]");
		} catch (Exception e) {
			LOGGER.error("update task error", e);
		}
		return super.scheduleWithFixedDelay(taskWrapper(task), startTime, delay);
	}

	
	/**
     * 使用fixedDelay的方式提交任务调度请求
     * <pre>
     * 任务首次启动时间未设置，任务池将会尽可能早的启动任务 
     * </pre>
     * @param task 待执行任务
     * @param delay 上一次任务结束时间与下一次任务开始时间的间隔时间，单位默认是毫秒 
     * @return 任务句柄
     */
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
		try {
			TaskDefine taskDefine = resolveTaskName(task);
			taskDefine.setPeriod(delay);
			checkScheduleDataManager();
			boolean rt = isUncodeTask(task);
			if(rt == false){
				scheduleDataManager.addTask(taskDefine);
			}
			LOGGER.debug(currenScheduleServer.getUuid() +":自动向集群注册任务[" + taskDefine.getSingalKey() + "]");
		} catch (Exception e) {
			LOGGER.error("update task error", e);
		}
		return super.scheduleWithFixedDelay(taskWrapper(task), delay);
	}
	
	public boolean checkAdminUser(String account, String password){
		if(StringUtils.isBlank(account) || StringUtils.isBlank(password)){
			return false;
		}
		String name = zkConfig.get(ZKManager.KEYS.userName.key);
		String pwd = zkConfig.get(ZKManager.KEYS.password.key);
		if(account.equals(name) && password.equals(pwd)){
			return true;
		}
		return false;
	}
	
	public String getScheduleServerUUid(){
		if(null != currenScheduleServer){
			return currenScheduleServer.getUuid();
		}
		return null;
	}

	public Map<String, Boolean> getIsOwnerMap() {
		return isOwnerMap;
	}

	public static ApplicationContext getApplicationcontext() {
		return ZKScheduleManager.applicationcontext;
	}
	
	public void setInitTaskDefines(List<TaskDefine> initTaskDefines) {
		this.initTaskDefines = initTaskDefines;
	}

	public void destroy() {
		try {
			if (this.initialThread != null) {
				this.initialThread.stopThread();
			}

			if (this.scheduleDataManager != null) {
				this.scheduleDataManager.clearExpireScheduleServer();
			}
			if (this.hearBeatTimer != null) {
				this.hearBeatTimer.cancel();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (this.zkManager != null) {
				try {
					this.zkManager.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	

}