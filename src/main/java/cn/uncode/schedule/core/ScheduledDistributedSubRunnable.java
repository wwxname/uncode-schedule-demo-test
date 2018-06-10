package cn.uncode.schedule.core;

import cn.uncode.schedule.ConsoleManager;
import cn.uncode.schedule.DynamicTaskManager;
import cn.uncode.schedule.ZKScheduleManager;
import cn.uncode.schedule.util.ScheduleUtil;
import cn.uncode.schedule.zk.DistributedQueue;
import cn.uncode.schedule.zk.TimestampTypeAdapter;
import com.google.gson.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduledDistributedSubRunnable implements Runnable {
	
	private static transient Logger LOG = LoggerFactory.getLogger(ScheduledDistributedSubRunnable.class);
	
	private Gson gson = new GsonBuilder().registerTypeAdapter(Timestamp.class,new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();

	private final TaskDefine taskDefine;
	
	private AtomicInteger count = new AtomicInteger();
	
	private AtomicInteger runTimes = new AtomicInteger();
	
	private AtomicInteger noneCount = new AtomicInteger();
	
	private DistributedQueue distributedQueue = null;
	
	private DistributedQueue checkDistributedQueue = null;
	
	public ScheduledDistributedSubRunnable(TaskDefine taskDefine) {
		this.taskDefine = taskDefine;
	}
	
	public int getRunTimes(){
		return runTimes.get();
	}

	public TaskDefine getTaskDefine() {
		return taskDefine;
	}

	@Override
	public void run(){
		Object bean = null;
		Method method = null;
		try {
			distributedQueue = ConsoleManager.getScheduleManager().getScheduleDataManager().buildDistributedQueue(taskDefine.getMutilMainKey());
			String checkName = ScheduleUtil.buildDoubleCheckDistributedName(taskDefine.getMutilMainKey());
			checkDistributedQueue = ConsoleManager.getScheduleManager().getScheduleDataManager().buildDistributedQueue(checkName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String[] datas = null;
		try {
			datas = distributedQueue.poll();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(null != datas){
			String size = null;
			try {
				size = checkDistributedQueue.get(datas[1]);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			int index = 0;
			if(StringUtils.isNotBlank(size)){
				index = Integer.valueOf(size);
			}
			
			bean = ZKScheduleManager.getApplicationcontext().getBean(taskDefine.getTargetBean());
			method = DynamicTaskManager.getMethod(bean, taskDefine.getTargetMethod(), String.class);
			ReflectionUtils.makeAccessible(method);
			if(StringUtils.isNotBlank(datas[0])){
				JsonArray jsonArray = new JsonParser().parse(datas[0]).getAsJsonArray();
				int total = jsonArray.size();
				for(;index<total;index++){
					JsonElement item = jsonArray.get(index);
					String param = gson.toJson(item);
					try{
						method.invoke(bean, param);
					}catch(Exception e){
						LOG.error("分布式任务调用错误，bean:"+bean.getClass() + ", method:" + method.getName()+", 参数：" + param, e);
					}
					try {
						count.incrementAndGet();
						runTimes.incrementAndGet();
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//重要存储节点，重试三次
				for(int i=0;i<3;i++){
					try {
						checkDistributedQueue.offer(datas[1], count.get());
						count.set(0);
						break;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}else{
			noneCount.incrementAndGet();
			//5次为空检查总任务状态，如果总任务结束，停止当前任务
			if(noneCount.get() > 5){
				String rt = null;
				try {
					rt = checkDistributedQueue.get(TaskDefine.STATUS_DISTRIBUTED_KEY);
				} catch (Exception e) {
					e.printStackTrace();
				}
				if(null != rt){
					rt = rt.replaceAll("\"", "");
					if(TaskDefine.STATUS_DISTRIBUTED_ALL_SUB_TASK_OVER.equals(rt)){
						try {
							ConsoleManager.getScheduleManager().getScheduleDataManager().delTask(taskDefine);
						} catch (Exception e) {
							e.printStackTrace();
						}
						DynamicTaskManager.stopTask(taskDefine.stringKey());
					}
				}
			}
		}
		
		
	}

}
