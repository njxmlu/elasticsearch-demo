package com.iflytek.es.util;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * ES 工具类
 * 
 * @desc: visitors-capacity-web
 * @author: kpchen@iflytek.com
 * @createTime: 2016年8月22日 上午9:16:33
 * @history:
 * @version: v1.0
 */

public class ESHandler {

	/**
	 * master节点实例名称
	 */
//	private static String clusterName = "zhjt_cluster";
	private static String clusterName = "bigdata-research";
	/**
	 * master ip and port
	 */
//	private static String masterIpAndPort = "172.31.8.6:9300";
	private static String masterIpAndPort = "192.168.86.58:9300";
	/**
	 * node ip and port
	 */
	private static String nodeIpAndPort = "";

	/**
	 * es客户端
	 */
	private static TransportClient client;
	
	

	private ESHandler() {
	}



	// 实例化client
	static {
		try {
			// ip与端口号分离
			if (null == masterIpAndPort || "".equals(masterIpAndPort)) {
				throw new RuntimeException("master接口IP或端口号配置error");
			}
			String masterIp = masterIpAndPort.split(":")[0];
			Integer masterPort = Integer.valueOf(masterIpAndPort.split(":")[1]);

			// 设置集群名称：默认是elasticsearch，并设置client.transport.sniff为true，使客户端嗅探整个集群状态，把集群中的其他机器IP加入到客户端中
			Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName)
					.put("client.transport.sniff", true).build();

			// 创建client
			client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(masterIp), masterPort));
			// 添加集群其他node
			if (null != nodeIpAndPort && !"".equals(nodeIpAndPort)) {
				String[] ipAndPorts = nodeIpAndPort.split(",");
				for (String str : ipAndPorts) {
					ESHandler.addNode(str.split(":")[0], Integer.valueOf(str.split(":")[1]));
				}
			}
		} catch (UnknownHostException e) {
			System.out.println("创建es客户端error ： " + e.getMessage());
		}
	}


	/**
	 * 为集群添加新的节点
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2016年8月22日 下午3:46:19
	 * @history:
	 * @param name
	 *            ：ip
	 * @param port
	 *            :端口号 void
	 */
	public static synchronized void addNode(String name, Integer port) {
		try {
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(name), port));
		} catch (UnknownHostException e) {
			System.out.println("添加es节点error ： " + e.getMessage());
		}
	}

	/**
	 * 删除集群中的某个节点
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2016年8月22日 下午3:46:11
	 * @history:
	 * @param name
	 *            ：ip
	 * @param port
	 *            :端口号 void
	 */
	public static synchronized void removeNode(String name, Integer port) {
		try {
			client.removeTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(name), port));
		} catch (UnknownHostException e) {
			System.out.println("移除es节点error ： " + e.getMessage());
		}
	}

	/**
	 * 取得实例
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2016年8月22日 上午9:21:11
	 * @history:
	 * @return TransportClient
	 */
	public static synchronized TransportClient getTransportClient() {
		return client;
	}

}