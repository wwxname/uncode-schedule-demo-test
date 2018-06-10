package cn.uncode.schedule.core;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;

/**
 * 任务定义，提供关键信息给使用者
 * @author juny.ye
 *
 */
public class TaskDefine {
	
	public static final String TYPE_UNCODE_SINGLE_TASK = "uncode-single-task";
	public static final String TYPE_UNCODE_MULTI_MAIN_TASK = "uncode-multi-main-task";
	public static final String TYPE_UNCODE_MULTI_SUB_TASK = "uncode-multi-sub-task";
	public static final String TYPE_SPRING_TASK = "spring-task";
	public static final String TYPE_QUARTZ_TASK = "quartz";
	
	public static final String STATUS_ERROR = "error";
	public static final String STATUS_STOP = "stop";
	public static final String STATUS_RUNNING = "running";
	public static final String STATUS_DISTRIBUTED_ALL_SUB_TASK_OVER = "subok";
	public static final String STATUS_DISTRIBUTED_KEY = "status";
	
    /**
     * 目标bean
     */
    private String targetBean;
    
    /**
     * 目标方法
     */
    private String targetMethod;
    
    
    /**
     * cron表达式
     */
    private String cronExpression;
	
	/**
	 * 开始时间
	 */
	private Date startTime;
	
	/**
	 * 两次任务启动时间之间的间隔时间（毫秒）
	 */
	private long period;
	
	/**
	 * 上一次任务结束时间与下一次任务开始时间的间隔时间（毫秒）
	 */
	private long delay;
	
	/**
	 * 参数
	 */
	private String params;
	
	/**
	 * 类型
	 */
	private String type;
	
	/**
	 * 后台显示参数，当前任务执行节点
	 */
	private String currentServer;
	
	/**
	 * 后台显示参数，无业务内含
	 */
	private int runTimes;
	
	/**
	 * 后台显示参数，无业务内含
	 */
	private long lastRunningTime;
	
	/**
	 * 后台显示参数，无业务内含
	 */
	private String status = STATUS_RUNNING;
	
	/**
	 * 执行比例
	 */
	private String percentage;
	
	/**
	 * key的后缀
	 */
	private String extKeySuffix;
	
	//--------------------------------
	// 分布式任务
	//--------------------------------
	
	/**
	 * 任务开始前调用方法
	 */
	private String beforeMethod;
	
	/**
	 * 任务结束后调用方法
	 */
	private String afterMethod;
	
	/**
	 * 线程数量
	 */
	private int threadNum = 1;
	
	/**
	 * 分布式子任务后缀
	 */
	private String subSuffix;
	
	/**
	 * spring bean名称
	 * @return bean名称
	 */
	public String getTargetBean() {
		return targetBean;
	}

	/**
	 * spring bean名称
	 * @param targetBean
	 */
	public void setTargetBean(String targetBean) {
		this.targetBean = targetBean;
	}

	/**
	 * 任务执行方法
	 * @return
	 */
	public String getTargetMethod() {
		return targetMethod;
	}
	
	/**
	 * 任务方法显示方
	 * @return
	 */
	public String getTargetMethod4Show(){
		if(StringUtils.isNotBlank(extKeySuffix)){
			return targetMethod + "-" + extKeySuffix;
		}
		return targetMethod;
	}

	/**
	 * 任务执行方法
	 * @param targetMethod
	 */
	public void setTargetMethod(String targetMethod) {
		if(StringUtils.isNotBlank(targetMethod) && targetMethod.indexOf("-") != -1){
			String[] vals = targetMethod.split("-");
			this.targetMethod = vals[0];
			this.extKeySuffix = vals[1];
		}else{
			this.targetMethod = targetMethod;
		}
	}

	/**
	 * 任务cron表达式
	 * @return
	 */
	public String getCronExpression() {
		return cronExpression;
	}

	/**
	 * 任务cron表达式
	 * @param cronExpression
	 */
	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	/**
	 * 分布式任务执行比例
	 * @return
	 */
	public String getPercentage() {
		return percentage;
	}

	/**
	 * 分布式任务执行比例
	 * @param percentage
	 */
	public void setPercentage(String percentage) {
		this.percentage = percentage;
	}

	/**
	 * 任务开始时间
	 * @return
	 */
	public Date getStartTime() {
		return startTime;
	}

	/**
	 * 任务开始时间
	 * @param startTime
	 */
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	/**
	 * 任务间隔时间
     * <pre>
	 * 两次任务启动时间之间的间隔时间，默认单位是毫秒
     * </pre>
	 * @return
	 */
	public long getPeriod() {
		return period;
	}

	/**
	 * 任务间隔时间
     * <pre>
	 * 两次任务启动时间之间的间隔时间，默认单位是毫秒
     * </pre>
	 * @param period
	 */
	public void setPeriod(long period) {
		this.period = period;
	}
	
	/**
	 * 任务间隔时间
     * <pre>
	 * 上一次任务结束时间与下一次任务开始时间的间隔时间，单位默认是毫秒 
     * </pre>
	 * @return
	 */
	public long getDelay() {
		return delay;
	}

	/**
	 * 任务间隔时间
     * <pre>
	 * 上一次任务结束时间与下一次任务开始时间的间隔时间，单位默认是毫秒 
     * </pre>
	 * @param delay
	 */
	public void setDelay(long delay) {
		this.delay = delay;
	}

	/**
	 * 执行任务的当前节点
	 * @return
	 */
	public String getCurrentServer() {
		return currentServer;
	}

	/**
	 * 执行任务的当前节点
	 * @param currentServer
	 */
	public void setCurrentServer(String currentServer) {
		this.currentServer = currentServer;
	}
	
	/**
	 * 获取任务标识，根据不同的任务类型封装
	 * @return
	 */
	public String stringKey(){
		if(TYPE_UNCODE_MULTI_MAIN_TASK.equals(type)){
			return getMutilMainKey();
		}else if(TYPE_UNCODE_MULTI_SUB_TASK.equals(type)){
			return getMutilSubKey();
		}
		return getSingalKey();
	}
	
	/**
	 * 获取单任务标识
	 * @return
	 */
	public String getSingalKey(){
		String result = null;
		boolean notBlank = StringUtils.isNotBlank(getTargetBean()) && StringUtils.isNotBlank(getTargetMethod());
		if(notBlank){
			result = getTargetBean() + "#" + getTargetMethod();
		}
		if(StringUtils.isNotBlank(extKeySuffix)){
			result += "-" + extKeySuffix;
		}
		return result;
	}
	
	/**
	 * 获取分布式任务主任务标识
	 * @return
	 */
	public String getMutilMainKey(){
		String result = null;
		boolean notBlank = StringUtils.isNotBlank(getTargetBean()) 
				&& StringUtils.isNotBlank(getBeforeMethod()) && StringUtils.isNotBlank(getAfterMethod());
		if(notBlank){
			result = getTargetBean() + "#" + getBeforeMethod() + "!" + getAfterMethod();
		}
		if(StringUtils.isNotBlank(extKeySuffix)){
			result += "-" + extKeySuffix;
		}
		return result;
	}
	
	/**
	 * 获取分布式任务子任务标识
	 * @return
	 */
	public String getMutilSubKey(){
		String result = null;
		boolean notBlank = StringUtils.isNotBlank(getTargetBean()) && StringUtils.isNotBlank(getTargetMethod());
		if(notBlank){
			result = getTargetBean() + "#" + getTargetMethod();
		}
		if(StringUtils.isNotBlank(extKeySuffix)){
			result += "-" + extKeySuffix;
		}
		if(StringUtils.isNotBlank(subSuffix)){
			result += subSuffix;
		}
		return result;
	}

	/**
	 * 任务执行方法传参，只支持字符串类型
	 * @return
	 */
	public String getParams() {
		return params;
	}

	/**
	 * 任务执行方法传参，只支持字符串类型
	 * @param params 参数字符串
	 */
	public void setParams(String params) {
		this.params = params;
	}

	/**
	 * 任务类型
	 * @return
	 */
	public String getType() {
		return type;
	}

	/**
	 * 设置任务类型
	 * 
	 * @param type
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * 运行次数
	 * @return
	 */
	public int getRunTimes() {
		return runTimes;
	}

	/**
	 * 设置运行次数
	 * @param runTimes
	 */
	public void setRunTimes(int runTimes) {
		this.runTimes = runTimes;
	}

	/**
	 * 获取任务最后执行时间
	 * @return
	 */
	public long getLastRunningTime() {
		return lastRunningTime;
	}

	/**
	 * 设置任务最后执行时间
	 * @param lastRunningTime
	 */
	public void setLastRunningTime(long lastRunningTime) {
		this.lastRunningTime = lastRunningTime;
	}

	/**
	 * 任务是否停止状态
	 * @return 是或否
	 */
	public boolean isStop() {
		return STATUS_STOP.equals(this.status);
	}

	/**
	 * 任务停止
	 */
	public void setStop() {
		this.status = STATUS_STOP;
	}

	/**
	 * 任务执行状态
     * <pre>
	 * STATUS_ERROR/STATUS_STOP/STATUS_RUNNING
     * </pre>
	 * @return 任务执行状态
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * 设置任务执行状态
     * <pre>
	 * STATUS_ERROR/STATUS_STOP/STATUS_RUNNING
     * </pre>
	 * @param status 任务执行状态
	 */
	public void setStatus(String status) {
		this.status = status;
	}

	/**
	 * 相同任务多次执行时添加扩展名
	 * @return 扩展名
	 */
	public String getExtKeySuffix() {
		return extKeySuffix;
	}

	/**
	 * 相同任务多次执行时添加扩展名
	 * @param extKeySuffix 扩展名
	 */
	public void setExtKeySuffix(String extKeySuffix) {
		this.extKeySuffix = extKeySuffix;
	}
	
	
	/**
	 * 任务开始前自动调用方法名称
     * <pre>
	 * 该方法不可以传参，可以有返回值，必须为Collection类型
     * </pre>
	 * @return 方法名称
	 */
	public String getBeforeMethod() {
		return beforeMethod;
	}

	/**
	 * 任务开始前自动调用方法名称
     * <pre>
	 * 该方法不可以传参，可以有返回值，必须为Collection类型
     * </pre>
	 * @param beforeMethod 方法名称
	 */
	public void setBeforeMethod(String beforeMethod) {
		this.beforeMethod = beforeMethod;
	}

	/**
	 * 任务结束后自动调用方法名称
     * <pre>
	 * 该方法不可以传参，不可以有返回值
     * </pre>
	 * @return 方法名称
	 */
	public String getAfterMethod() {
		return afterMethod;
	}

	/**
	 * 任务结束后自动调用方法名称
     * <pre>
	 * 该方法不可以传参，不可以有返回值
     * </pre>
	 * @param afterMethod 方法名称
	 */
	public void setAfterMethod(String afterMethod) {
		this.afterMethod = afterMethod;
	}
	
	/**
	 * 任务线程数量
	 * @return 任务线程数量
	 */
	public int getThreadNum() {
		return threadNum;
	}

	/**
	 * 多线程任务数量
	 * @param threadNum 任务线程数量
	 */
	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}
	/**
	 * 获取分布式任务子任务后缀名
	 * @return
	 */
	public String getSubSuffix() {
		return subSuffix;
	}

	/**
	 * 设置分布式任务子任务后缀名
	 * @param subSuffix 后缀名
	 */
	public void setSubSuffix(String subSuffix) {
		this.subSuffix = subSuffix;
	}

	/**
	 * 创建分布式任务子任务
	 * @param queue
	 * @param paramJson
	 * @return
	 */
	public TaskDefine buildDistributedSubTask(String queue, String paramJson){
		TaskDefine subTaskDefine = new TaskDefine();
		subTaskDefine.valueOf(this);
		subTaskDefine.setSubSuffix(queue);
		subTaskDefine.setParams(paramJson);
		subTaskDefine.setType(TaskDefine.TYPE_UNCODE_MULTI_SUB_TASK);
		subTaskDefine.setThreadNum(1);
		return subTaskDefine;
	}

	/**
	 * 将源对象所有不为空的属性复制到当前对象中
	 * @param source 源对象
	 */
	public void valueOf(TaskDefine source){
		if(StringUtils.isNotBlank(source.getTargetBean())){
			this.targetBean = source.getTargetBean();
		}
		if(StringUtils.isNotBlank(source.getTargetMethod())){
			if(source.getTargetMethod().indexOf("-") != -1){
				String[] vals = source.getTargetMethod().split("-");
				this.targetMethod = vals[0];
				this.extKeySuffix = vals[1];
			}else{
				this.targetMethod = source.getTargetMethod();
			}
		}
		if(StringUtils.isNotBlank(source.getCronExpression())){
			this.cronExpression = source.getCronExpression();
		}
		if(source.getStartTime() != null){
			this.startTime = source.getStartTime();
		}
		if(source.getPeriod() > 0){
			this.period = source.getPeriod();
		}
		if(source.getDelay() > 0){
			this.delay = source.getDelay();
		}
		if(StringUtils.isNotBlank(source.getParams())){
			this.params = source.getParams();
		}
		if(StringUtils.isNotBlank(source.getType())){
			this.type = source.getType();
		}
		if(StringUtils.isNotBlank(source.getStatus())){
			this.status = source.getStatus();
		}
		if(StringUtils.isNotBlank(source.getExtKeySuffix())){
			this.extKeySuffix = source.getExtKeySuffix();
		}
		if(StringUtils.isNotBlank(source.getBeforeMethod())){
			this.beforeMethod = source.getBeforeMethod();
		}
		if(StringUtils.isNotBlank(source.getAfterMethod())){
			this.afterMethod = source.getAfterMethod();
		}
		if(StringUtils.isNotBlank(source.getSubSuffix())){
			this.subSuffix = source.getSubSuffix();
		}
		if(source.getThreadNum() > 1){
			this.threadNum = source.getThreadNum();
		}
	}


    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((targetBean == null) ? 0 : targetBean.hashCode());
        result = prime * result + ((targetMethod == null) ? 0 : targetMethod.hashCode());
        result = prime * result + ((cronExpression == null) ? 0 : cronExpression.hashCode());
        result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
        result = prime * result + (int)period;
        result = prime * result + (int)delay;
        result = prime * result + ((params == null) ? 0 : params.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((extKeySuffix == null) ? 0 : extKeySuffix.hashCode());
        result = prime * result + ((beforeMethod == null) ? 0 : beforeMethod.hashCode());
        result = prime * result + ((afterMethod == null) ? 0 : afterMethod.hashCode());
        result = prime * result + ((subSuffix == null) ? 0 : subSuffix.hashCode());
        result = prime * result + threadNum;
        return result;
    }
    
	@SuppressWarnings("deprecation")
	@Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TaskDefine)) {
            return false;
        }
        TaskDefine ou = (TaskDefine) obj;
        if (!ObjectUtils.equals(this.targetBean, ou.targetBean)) {
            return false;
        }
        if (!ObjectUtils.equals(this.targetMethod, ou.targetMethod)) {
            return false;
        }
        if (!ObjectUtils.equals(this.cronExpression, ou.cronExpression)) {
            return false;
        }
        if (!ObjectUtils.equals(this.startTime, ou.startTime)) {
            return false;
        }
        if (!ObjectUtils.equals(this.params, ou.params)) {
            return false;
        }
        if (!ObjectUtils.equals(this.type, ou.type)) {
            return false;
        }
        if (!ObjectUtils.equals(this.extKeySuffix, ou.extKeySuffix)) {
            return false;
        }
        if (!ObjectUtils.equals(this.beforeMethod, ou.beforeMethod)) {
            return false;
        }
        if (!ObjectUtils.equals(this.afterMethod, ou.afterMethod)) {
            return false;
        }
        if (!ObjectUtils.equals(this.subSuffix, ou.subSuffix)) {
            return false;
        }
        return this.period == ou.period 
        		&& this.delay == ou.delay
        		&& this.threadNum == ou.threadNum;
    }
	
	
	
	
}