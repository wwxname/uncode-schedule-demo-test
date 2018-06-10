package cn.uncode.schedule;

import cn.uncode.schedule.core.TaskDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class ConsoleManager {
	
    private static transient Logger log = LoggerFactory.getLogger(ConsoleManager.class);
    
//    private static Gson GSON = new GsonBuilder().create();

    private static ZKScheduleManager scheduleManager;
    
    static Properties properties = new Properties();
    
    public static void setProperties(Properties prop){
    	properties.putAll(prop);
    }
    
    public static ZKScheduleManager getScheduleManager() throws Exception {
    	if(null == ConsoleManager.scheduleManager){
			synchronized(ConsoleManager.class) {
				ConsoleManager.scheduleManager = ZKScheduleManager.getApplicationcontext().getBean(ZKScheduleManager.class);
			}
    	}
        return ConsoleManager.scheduleManager;
    }

    /**
     * 添加任务
     * @param taskDefine 任务定义
     */
    public static void addScheduleTask(TaskDefine taskDefine) {
        try {
        	log.info("添加任务："+taskDefine.getSingalKey());
			ConsoleManager.getScheduleManager().getScheduleDataManager().addTask(taskDefine);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }
    
    /**
     * 删除任务
     * @param taskDefine 任务定义
     */
    public static void delScheduleTask(TaskDefine taskDefine) {
        try {
			ConsoleManager.scheduleManager.getScheduleDataManager().delTask(taskDefine);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }
    
    /**
     * 不可用
     * @param targetBean bean名称
     * @param targetMethod 方法名称
     */
    @Deprecated
    public static void delScheduleTask(String targetBean, String targetMethod) {
        try {
			ConsoleManager.scheduleManager.getScheduleDataManager().delTask(targetBean, targetMethod);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }
    
    /**
     * 修改任务
     * @param taskDefine 任务定义
     */
    public static void updateScheduleTask(TaskDefine taskDefine) {
        try {
			ConsoleManager.scheduleManager.getScheduleDataManager().updateTask(taskDefine);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
    }
    
    /**
     * 查询所有任务列表
     * @return 任务列表
     */
    public static List<TaskDefine> queryScheduleTask() {
    	List<TaskDefine> taskDefines = new ArrayList<TaskDefine>();
        try {
			List<TaskDefine> tasks = ConsoleManager.getScheduleManager().getScheduleDataManager().selectTask();
			taskDefines.addAll(tasks);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
        return taskDefines;
    }
    
    /**
     * 任务是否存在
     * @param taskDefine  任务定义
     * @return 是或否
     * @throws Exception 异常
     */
    public static boolean isExistsTask(TaskDefine taskDefine) throws Exception{
    		return ConsoleManager.scheduleManager.getScheduleDataManager().isExistsTask(taskDefine);
    }
    
    /**
     * 根据标识查询相关任务
     * @param taskDefine 任务定义
     * @return 任务信息
     * @throws Exception 异常
     */
    public static TaskDefine queryScheduleTask(TaskDefine taskDefine) throws Exception{
		return ConsoleManager.scheduleManager.getScheduleDataManager().selectTask(taskDefine);
    }
    
    /**
     * 判断当前任务是否属于当前节点
     * @param taskDefine 任务定义
     * @return 是或否
     * @throws Exception 异常
     */
    public static boolean isOwner(TaskDefine taskDefine) throws Exception{
		return ConsoleManager.scheduleManager.getScheduleDataManager().isOwner(taskDefine.getSingalKey(),
				ConsoleManager.getScheduleManager().getScheduleServerUUid());
    }
    
}
