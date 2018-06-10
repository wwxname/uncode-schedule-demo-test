package cn.uncode.schedule.core;

import cn.uncode.schedule.ConsoleManager;
import cn.uncode.schedule.DynamicTaskManager;
import cn.uncode.schedule.ZKScheduleManager;
import cn.uncode.schedule.util.ScheduleUtil;
import cn.uncode.schedule.zk.DistributedQueue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduledDistributedMainRunnable implements Runnable {
	
	private static transient Logger LOG = LoggerFactory.getLogger(ScheduledDistributedMainRunnable.class);
	
	private static final int THREAD_SIZE = 5;
	
	private static final int DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE = 100;

	private final TaskDefine taskDefine;
	
	private List<?> data = null;
	
	private int threadSize = THREAD_SIZE;
	
	private AtomicInteger count = new AtomicInteger();
	
	private DistributedQueue distributedQueue = null;
	
	private DistributedQueue checkDistributedQueue = null;
	
	private Set<String> subThreadTaskName = new HashSet<String>();
	
	public ScheduledDistributedMainRunnable(TaskDefine taskDefine) {
		this.taskDefine = taskDefine;
	}

	public TaskDefine getTaskDefine() {
		return taskDefine;
	}
	
	public int getRunTimes(){
		return count.get();
	}

	@Override
	public void run() {
		
			if(StringUtils.isNotBlank(taskDefine.getBeforeMethod()) 
					&& TaskDefine.TYPE_UNCODE_MULTI_MAIN_TASK.equals(taskDefine.getType())){
				Object bean = null;
				Method method = null;
				boolean isReadDataAgain = true;
				try {
					distributedQueue = ConsoleManager.getScheduleManager().getScheduleDataManager().buildDistributedQueue(taskDefine.stringKey());
					String checkName = ScheduleUtil.buildDoubleCheckDistributedName(taskDefine.getMutilMainKey());
					checkDistributedQueue = ConsoleManager.getScheduleManager().getScheduleDataManager().buildDistributedQueue(checkName);
					boolean taskProcedeAlready = checkDistributedQueue != null && checkDistributedQueue.size() > 0;
					isReadDataAgain = taskProcedeAlready == false;
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				int total = 0;
				//执行before方法读数据，在未开始执行前可以重试
				if(isReadDataAgain){
					bean = ZKScheduleManager.getApplicationcontext().getBean(taskDefine.getTargetBean());
					method = DynamicTaskManager.getMethod(bean, taskDefine.getBeforeMethod());
					boolean flag = readDataFromBeforeMethod(bean, method);
					if(flag){
						if(null != data && data.size() > 0){
							total = data.size();
							int index = 0;
							List<?> subList = null;
							String key = null;
							int etotal = total / DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE;
							if(total % DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE != 0){
								etotal += 1;
							}
							int lastTimes = 0;
							while(index < total){
								boolean last = false;
								if(index + DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE < total){
									subList = data.subList(index, index + DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE);
									key = String.valueOf(index + DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE);
								}else{
									subList = data.subList(index, data.size());
									key = String.valueOf(total);
									last = true;
								}
								try {
									distributedQueue.offer(key, subList);
								} catch (Exception e) {
									e.printStackTrace();
								}
								index += DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE;
								if(last){
									if(etotal <= distributedQueue.size()){
										break;
									}else{
										index = 0;
									}
									lastTimes++;
								}
								if(lastTimes > 3){
									break;
								}
							}
						}
					}
				}
					
				//按照线程数添加需要执行的目标任务到集群节点上
				boolean taskExsist = distributedQueue != null && distributedQueue.size() > 0;
				if(taskExsist){
					if(taskDefine.getThreadNum() > 0){
						threadSize = taskDefine.getThreadNum();
					}
					for(int i = 0; i < threadSize; i++){
						TaskDefine subTaskDefine  = taskDefine.buildDistributedSubTask(""+(i+1), null);
						try {
							ConsoleManager.getScheduleManager().getScheduleDataManager().addTask(subTaskDefine);
							subThreadTaskName.add(subTaskDefine.stringKey());
						} catch (Exception e) {
							LOG.error("Distributed queue add sub task error, [name:"+subTaskDefine.stringKey()
							+",key:"+(i+1)+"]", e);
						}
					}	
				}
				
				//线程等待直到所有任务执行完成
				int constrainQuitCount = 0;
				while(null != distributedQueue && distributedQueue.size() >= 0){
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					constrainQuitCount++;
					//任务超过1小时强制退出
					if(constrainQuitCount >= 6*60){
						break;
					}
					
					if(null == data){
						bean = ZKScheduleManager.getApplicationcontext().getBean(taskDefine.getTargetBean());
						method = DynamicTaskManager.getMethod(bean, taskDefine.getBeforeMethod());
						boolean flag = readDataFromBeforeMethod(bean, method);
						if(flag){
							total = data.size();
						}
					}
					
					//检查任务是否遗漏
					if(distributedQueue.size() == 0){
						//计算queue长度
						int etotal = total / DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE;
						if(total % DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE != 0){
							etotal += 1;
						}
						//任务有遗漏
						if(etotal > checkDistributedQueue.size()){
							if(null != data && data.size() > 0){
								int index = 0;
								List<?> subList = null;
								String key = null;
								
								while(index < total){
									if(index + DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE < total){
										subList = data.subList(index, index + DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE);
										key = String.valueOf(index + DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE);
									}else{
										subList = data.subList(index, data.size());
										key = String.valueOf(total);
									}
									boolean exist = false;
									try {
										exist = checkDistributedQueue.exist(key);
										if(exist == false){
											distributedQueue.offer(key, subList);
										}
									} catch (Exception e) {
										e.printStackTrace();
									}
									index += DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE;
								}
							}
						}
						
						if(checkDistributedQueue.size() >= etotal){
							try {
								ConsoleManager.getScheduleManager().getScheduleDataManager()
								.saveRunningInfo(taskDefine.stringKey(), ConsoleManager.getScheduleManager().getScheduleServerUUid(), -1, "100%");
							} catch (Exception e) {
								LOG.error("Zookeeper save running info error", e);
							}
							break;
						}
					}else{
						//计算queue长度
						int etotal = total / DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE;
						if(total % DISTRIBUTE_QUEUE_BLOCK_DATA_SIZE != 0){
							etotal += 1;
						}
						int percentage = ((etotal-distributedQueue.size())*100)/etotal;
						try {
							ConsoleManager.getScheduleManager().getScheduleDataManager()
							.saveRunningInfo(taskDefine.stringKey(), ConsoleManager.getScheduleManager().getScheduleServerUUid(), -1, percentage+"%");
						} catch (Exception e) {
							LOG.error("Zookeeper save running info error", e);
						}
					}
				}
				
				//执行after方法
				if(StringUtils.isBlank(taskDefine.getAfterMethod())){
					try {
						ConsoleManager.getScheduleManager().getScheduleDataManager()
						.saveRunningInfo(taskDefine.stringKey(), ConsoleManager.getScheduleManager().getScheduleServerUUid(), -1, "after method is null");
					} catch (Exception e) {
						LOG.error("Zookeeper save running info error", e);
					}
				}else{
					String status = null;
					try {
						status = checkDistributedQueue.get(TaskDefine.STATUS_DISTRIBUTED_KEY);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					try {
						if(null == status){
							bean = ZKScheduleManager.getApplicationcontext().getBean(taskDefine.getTargetBean());
							method = DynamicTaskManager.getMethod(bean, taskDefine.getAfterMethod());
							ReflectionUtils.makeAccessible(method);
							method.invoke(bean);
							//重要存储节点，重试三次
							for(int i=0;i<3;i++){
								try {
									checkDistributedQueue.offer(TaskDefine.STATUS_DISTRIBUTED_KEY, TaskDefine.STATUS_DISTRIBUTED_ALL_SUB_TASK_OVER);
									break;
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}catch (InvocationTargetException ex) {
						ReflectionUtils.rethrowRuntimeException(ex.getTargetException());
					}catch (IllegalAccessException ex) {
						throw new UndeclaredThrowableException(ex);
					}
				}
				
				//任务清理工作
				if(subThreadTaskName.size() == 0){
					if(taskDefine.getThreadNum() > 0){
						threadSize = taskDefine.getThreadNum();
					}
					for(int i = 0; i < threadSize; i++){
						TaskDefine subTaskDefine  = taskDefine.buildDistributedSubTask(""+(i+1), null);
						subThreadTaskName.add(subTaskDefine.stringKey());
					}	
				}
				subThreadTaskName.add(taskDefine.stringKey());
				String[] arr = new String[]{};
				arr = subThreadTaskName.toArray(arr);
				for(int i=0;i<5;i++){
					int dsize = 1;
					try {
						dsize = ConsoleManager.getScheduleManager().getScheduleDataManager().delDistributedTask(arr);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					if(dsize == 0){
						break;
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				distributedQueue.clear();
			}else{
				try {
					ConsoleManager.getScheduleManager().getScheduleDataManager()
					.saveRunningInfo(taskDefine.stringKey(), ConsoleManager.getScheduleManager().getScheduleServerUUid(), -1, "before method is null");
				} catch (Exception e) {
					LOG.error("Zookeeper save running info error", e);
				}
			}
			count.incrementAndGet();
		}
		
					
		private boolean readDataFromBeforeMethod(Object bean, Method method){
			try {
				ReflectionUtils.makeAccessible(method);
				Object list = method.invoke(bean);
				if(null != list && list instanceof List){
					data = (List<?>) list;
					return true;
				}
			}catch (InvocationTargetException ex) {
				ReflectionUtils.rethrowRuntimeException(ex.getTargetException());
			}catch (IllegalAccessException ex) {
				throw new UndeclaredThrowableException(ex);
			}
			return false;
		}
		

}
