package cn.uncode.schedule.zk;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class DistributedQueue {
	
	private static transient Logger LOG = LoggerFactory.getLogger(DistributedQueue.class);
	
	public static final String NODE_DISTRIBUTE_QUEUE = "queue";
	
	private ZKManager zkManager = null;
    private String queuePath = null;
    
    private Gson gson = null;
  
    protected static final String ELEMENT_NAME = "e_";//顺序节点的名称 
      
  
  
    public DistributedQueue(ZKManager zkManager, String node) {
        this.zkManager = zkManager;  
        this.queuePath = this.zkManager.getRootPath() +"/" + NODE_DISTRIBUTE_QUEUE + "/" + node;
        this.gson = new GsonBuilder().registerTypeAdapter(Timestamp.class,new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();
        try {
			if(this.zkManager.getZooKeeper().exists(queuePath, false) == null){
				if(this.zkManager.getZooKeeper()
						.exists(this.zkManager.getRootPath() +"/" + NODE_DISTRIBUTE_QUEUE, false) == null){
					this.zkManager.getZooKeeper().create(this.zkManager.getRootPath() +"/" + NODE_DISTRIBUTE_QUEUE, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
				}
				this.zkManager.getZooKeeper().create(queuePath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			LOG.error("zk create distributed queue path error", e);
		} catch (InterruptedException e) {
			LOG.error("zk create distributed queue path error", e);
		} catch (Exception e) {
			LOG.error("zk create distributed queue path error", e);
		}
    }
      
    /** 
     * 获取队列的大小
     * <pre>
     * 通过获取根节点下的子节点列表 
     * </pre>
     */  
    public int size() {  
        return getChildren().size();
    }  
      
    //判断队列是否为空  
    public boolean isEmpty() {  
        return getChildren().size() == 0;  
    }
    
    /**
     * 获取子节点
     * @return
     */
    private List<String> getChildren(){
    	List<String> names = new ArrayList<>();
		try {
			List<String> list = this.zkManager.getZooKeeper().getChildren(this.queuePath,false);
			names.addAll(list);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return names;
    }
      
    /** 
     * 向队列添加数据 
     * @param element 
     * @return 
     * @throws Exception 
     */  
    public String offer(String name, Object element) throws Exception{  
    	if(this.zkManager.getZooKeeper().exists(queuePath + "/" +ELEMENT_NAME + name, false) == null){
    		String json = this.gson.toJson(element);
        	this.zkManager.getZooKeeper().create(queuePath + "/" +ELEMENT_NAME + name, json.getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
        	LOG.debug("存入key:"+name+"到队列中");
        	return json;
    	}
        return null;
    }
    
  //从队列取数据  
    public String get(String name) throws Exception{
    	String json = null;
    	if(this.zkManager.getZooKeeper().exists(queuePath + "/" +ELEMENT_NAME + name, false) != null){
    		byte[] data = this.zkManager.getZooKeeper().getData(queuePath + "/" +ELEMENT_NAME + name, null, null);
			if (null != data) {
				 json = new String(data);
			}
    	}
    	return json;
    }
    
    public boolean exist(String name){
    	boolean rt = false;
		try {
			rt = this.zkManager.getZooKeeper().exists(queuePath + "/" +ELEMENT_NAME + name, false) != null;
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return rt;
    }
  
  
    //从队列取数据  
    public String[] poll() throws Exception {  
        List<String> list = getChildren();  
        if (list.size() == 0) {  
            return null;  
        }  
        //将队列安装由小到大的顺序排序  
        Collections.sort(list, new Comparator<String>() {  
            public int compare(String lhs, String rhs) {  
                return getNodeNumber(lhs, ELEMENT_NAME).compareTo(getNodeNumber(rhs, ELEMENT_NAME));  
            }  
        });  
          
        /** 
         * 将队列中的元素做循环，然后构建完整的路径，在通过这个路径去读取数据 
         */
        String node = list.get(0);
        byte[] data = this.zkManager.getZooKeeper().getData(queuePath + "/" + node, null, null);
        String json = null;
		if (null != data) {
			 json = new String(data);
		}
		this.zkManager.getZooKeeper().delete(queuePath + "/" + node, -1);
          
        return new String[]{json,node.replace(ELEMENT_NAME, "")};  
    }  
  
      
    private String getNodeNumber(String str, String nodeName) {  
        int index = str.lastIndexOf(nodeName);  
        if (index >= 0) {  
            index += ELEMENT_NAME.length();  
            return index <= str.length() ? str.substring(index) : "";  
        }  
        return str;  
  
    }
    
    public boolean clear(){
    	List<String> names = getChildren();
    	if(null != names && names.size() > 0){
    		for(String name : names){
    			try {
					zkManager.getZooKeeper().delete(queuePath + "/" + name, -1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
    		}
    	}
    	try {
    		zkManager.getZooKeeper().delete(queuePath, -1);
			return true;
		} catch (Exception e) {
			LOG.error("Clear node: " + this.queuePath + "error.", e);
		}
    	return false;
    }

}
