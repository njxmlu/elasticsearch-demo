package com.iflytek.es.curd;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchHandler {
	// 客户端
	private Client client;

	public ElasticSearchHandler() {
		//
		try {
			// 设置集群名称：默认是elasticsearch，并设置client.transport.sniff为true，使客户端嗅探整个集群状态，把集群中的其他机器IP加入到客户端中
			Settings settings = Settings.settingsBuilder().put("cluster.name", "zhjt-kaifa-es").put("client.transport.sniff", true).build();

			// 创建client
			client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.31.8.18"), 9300));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 建立索引,索引建立好之后,会在elasticsearch\data\elasticsearch\nodes\0 <br/>
	 * {批量插入数据}
	 * 
	 * @param indexname
	 *            为索引库名，一个es集群中可以有多个索引库。 名称必须为小写
	 * @param type
	 *            Type为索引类型，是用来区分同索引库下不同类型的数据的，一个索引库下可以有多个索引类型。
	 * @param jsondata
	 *            json格式的数据集合
	 * 
	 * @return
	 */
	public void createIndexResponse(String indexname, String type, List<String> jsondata) {
		// 创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
		IndexRequestBuilder requestBuilder = client.prepareIndex(indexname, type).setRefresh(true);
		for (int i = 0; i < jsondata.size(); i++) {
			requestBuilder.setSource(jsondata.get(i)).execute().actionGet();
		}

	}


	/**
	 * 插入一行数据
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年2月27日 下午4:24:00
	 * @history:
	 * @param indexname
	 * @param type
	 * @param jsondata
	 * @return IndexResponse
	 */
	public IndexResponse createIndexResponse(String indexname, String type, String jsondata) {
		IndexResponse response = client.prepareIndex(indexname, type).setSource(jsondata).execute().actionGet();
		return response;
	}

	/**
	 * 执行搜索
	 * 
	 * @param queryBuilder
	 * @param indexname
	 * @param type
	 * @return
	 */
	public List<Sxtdept> searcher(QueryBuilder queryBuilder, String indexname, String type) {
		List<Sxtdept> list = new ArrayList<Sxtdept>();
		SearchResponse searchResponse = client.prepareSearch(indexname).setTypes(type).setQuery(queryBuilder).execute().actionGet();
		SearchHits hits = searchResponse.getHits();
		System.out.println("查询到记录数=" + hits.getTotalHits());
		SearchHit[] searchHists = hits.getHits();
		if (searchHists.length > 0) {
			for (SearchHit hit : searchHists) {
				Integer id = (Integer) hit.getSource().get("id");
				String name = (String) hit.getSource().get("name");
				String function = (String) hit.getSource().get("funciton");
				list.add(new Sxtdept(id, name, function));
			}
		}
		return list;
	}

	// 对象转json
	public static String obj2JsonData(Sxtdept sxtdept) {
		String jsonData = null;
		try {
			// 使用XContentBuilder创建json数据
			XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
			jsonBuild.startObject().field("id", sxtdept.getId()).field("name", sxtdept.getName()).field("funciton", sxtdept.getFunction()).endObject();
			jsonData = jsonBuild.string();
			// System.out.println(jsonData);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return jsonData;
	}

	// 组装数据
	public static List<String> getInitJsonData() {
		List<String> list = new ArrayList<String>();
		String data1 = obj2JsonData(new Sxtdept(1, "上海瑞元java部门", "功能：培训java，ssh三大框架"));
		String data2 = obj2JsonData(new Sxtdept(2, "上海瑞元前端部门", "功能：培训web前端，easyUI jQuery"));
		String data3 = obj2JsonData(new Sxtdept(3, "上海瑞元ios部门", "功能：培训苹果应用ios开发"));
		String data4 = obj2JsonData(new Sxtdept(4, "上海瑞元安卓部门", "功能：培训安卓手机应用开发"));
		String data5 = obj2JsonData(new Sxtdept(5, "上海瑞元UDE部门", "功能：设计页面"));
		String data6 = obj2JsonData(new Sxtdept(6, "上海瑞元大数据部门", "功能：培训数据挖掘 大数据云计算技术"));
		list.add(data1);
		list.add(data2);
		list.add(data3);
		list.add(data4);
		list.add(data5);
		list.add(data6);
		return list;
	}

	/**
	 * 删除一个索引
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年2月27日 下午4:27:07
	 * @history:
	 * @param indexname
	 *            void
	 */
	public void deleteIndexResponse(String indexname) {
		// 根据IndicesExistsResponse对象的isExists()方法的boolean返回值可以判断索引库是否存在
		IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexname);
		IndicesExistsResponse inExistsResponse = client.admin().indices().exists(inExistsRequest).actionGet();
		if (inExistsResponse.isExists()) {// 存在，删除
			client.admin().indices().prepareDelete(indexname).execute().actionGet();
		} else {
			System.out.println(indexname + "不存在");
		}
	}

	/**
	 * 删除一行数据
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年2月27日 下午4:27:14
	 * @history:
	 * @param indexname：索引名称
	 * @param type
	 *            ： 类型
	 * @param id：主键
	 *            void
	 */
	public void deleteIndexById(String indexname, String type, String id) {
		DeleteResponse dResponse = client.prepareDelete(indexname, type, id).execute().actionGet();
		if (dResponse.isFound()) {
			System.out.println("删除成功");
		} else {
			System.out.println("删除失败");
		}
	}

	public static void main(String[] args) {
		ElasticSearchHandler esHandler = new ElasticSearchHandler();
		System.out.println(esHandler);
		List<String> jsondata = getInitJsonData();
		String indexname = "indexdemo";
		String type = "typedemo";
		// String id = "AVp-wK-maxm6uS3a1l3_";
		esHandler.createIndexResponse(indexname, type, jsondata);
		// esHandler.deleteIndexById(indexname, type, id);

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "大数据");
		List<Sxtdept> result = esHandler.searcher(queryBuilder, indexname, type);
		for (int i = 0; i < result.size(); i++) {
			Sxtdept sxtdept = result.get(i);
			System.out.println("(" + sxtdept.getId() + ")部门名称:" + sxtdept.getName() + "\t\t" + sxtdept.getFunction());
		}
	}
}