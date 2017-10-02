/*
* 文件名：JestESUtil
* 版权：Copyright @ 2017 zhjt-web All Rights Reserved.
* 描述：
* 修改人：kpchen@iflytek.com
* 修改时间：2017年5月15日 下午4:39:20
* 修改内容：〈修改内容〉
*/
package com.iflytek.es.jestnew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Count;
import io.searchbox.core.CountResult;
import io.searchbox.core.Delete;
import io.searchbox.core.DeleteByQuery;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import io.searchbox.core.Update;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.indices.type.TypeExist;
import io.searchbox.params.Parameters;

/**
 * {jest 方式操作ES}
 *
 * @desc: zhjt-web
 * @author: kpchen@iflytek.com
 * @createTime: 2017年5月15日 下午4:39:20
 * @history:
 * @version: v1.0
 */
public class JestESUtil {

	/**
	 * es master 节点IP和端口号
	 */
	private String jestUrl="http://172.31.10.15:9200";

	/**
	 * {获取JestClient对象 }
	 *
	 * @return JestClient
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:12:08
	 * @history:
	 */
	public JestClient getJestClient() {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder(jestUrl)
				.readTimeout(60000)
				.multiThreaded(true) // 启动多线程
				.defaultMaxTotalConnectionPerRoute(2) //默认情况下，不超过2个并发
				.maxTotalConnection(20) // 最大连接数20
				.build());
		return factory.getObject();
	}

	/**
	 * {关闭JestClient客户端 }
	 *
	 * @param jestClient
	 * @throws Exception
	 *             void
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:33:47
	 * @history:
	 */
	public static void closeJestClient(JestClient jestClient) throws Exception {
		if (jestClient != null) {
			jestClient.shutdownClient();
		}
	}

	/**
	 * {创建索引 }
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @return
	 * @throws Exception
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:13:50
	 * @history:
	 */
	public static boolean createIndex(JestClient jestClient, String indexName) throws Exception {
		JestResult jr = jestClient.execute(new CreateIndex.Builder(indexName).build());
		return jr.isSucceeded();
	}

	/**
	 * {Put映射，模板配置}
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @param source:data
	 * @return
	 * @throws Exception
	 *             boolean
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:17:10
	 * @history:
	 */
	public static boolean createIndexMapping(JestClient jestClient, String indexName, String typeName, String source) throws Exception {
		PutMapping putMapping = new PutMapping.Builder(indexName, typeName, source).build();
		JestResult jr = jestClient.execute(putMapping);
		return jr.isSucceeded();
	}

	/**
	 * {Get映射，模板配置}
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @return
	 * @throws Exception
	 *             boolean
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:17:10
	 * @history:
	 */
	public static String getIndexMapping(JestClient jestClient, String indexName, String typeName) throws Exception {
		GetMapping getMapping = new GetMapping.Builder().addIndex(indexName).addType(typeName).build();
		JestResult jr = jestClient.execute(getMapping);
		return jr.getJsonString();
	}

	/**
	 * {索引文档 调用该接口的时候需要控制List<Object> objs 的数据量不要太大}
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @param objs
	 *            数据
	 * @return
	 * @throws Exception
	 *             boolean
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:19:51
	 * @history:
	 */
	public static boolean insertByBulk(JestClient jestClient, String indexName, String typeName, List<Object> objs) throws Exception {
		Bulk.Builder bulk = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName);
		for (Object obj : objs) {
			Index index = new Index.Builder(obj).refresh(true).build();
			bulk.addAction(index);
		}
		BulkResult br = jestClient.execute(bulk.refresh(true).build());
		return br.isSucceeded();
	}

	/**
	 * 批量插入数据
	 * 
	 * @author: hdzhang@iflytek.com
	 * @createTime: 2017年5月22日 下午6:33:39
	 * @history:
	 * @param jestClient
	 *            jestClient
	 * @param indexName
	 *            索引名
	 * @param typeName
	 *            文档名
	 * @param list
	 *            待插入数据集合
	 * @return
	 * @throws IOException
	 *             boolean
	 */
	public static <T> boolean insert(JestClient jestClient, String indexName, String typeName, List<T> list) throws IOException {
		Bulk.Builder bulk = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName);
		for (T t : list) {
			bulk.addAction(new Index.Builder(t).refresh(true).build());
		}
		JestResult jestResult = jestClient.execute(bulk.refresh(true).build());
	
		// System.out.println(jestResult.getErrorMessage());
		// System.out.println(jestResult.getJsonString());
	
		return jestResult.isSucceeded();
	}

	/**
	 * {搜索文档 默认查询10条}
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @param query
	 *            查询条件
	 * @return
	 * @throws Exception
	 *             SearchResult
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:29:14
	 * @history:
	 */
	public static SearchResult search(JestClient jestClient, String indexName, String typeName, String query) throws Exception {
		Search search = new Search.Builder(query).addIndex(indexName).addType(typeName).setParameter(Parameters.SCROLL, "1m").build();
		return jestClient.execute(search);
	}

	/**
	 * {搜索文档}
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @param query
	 *            查询条件
	 * @return
	 * @throws Exception
	 *             SearchResult
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:29:14
	 * @history:
	 */
	public static SearchResult search(JestClient jestClient, String indexName, String typeName, String query, Integer pageSize) throws Exception {
		Search search = new Search.Builder(query).addIndex(indexName).addType(typeName).setParameter(Parameters.SCROLL, "1m").setParameter(Parameters.SIZE, pageSize).build();
		return jestClient.execute(search);
	}

	/**
	 * 搜索文档
	 *
	 * @author: pyma@iflytek.com
	 * @createTime: 2017年6月23日 上午9:12:44
	 * @history:
	 * @param jestClient
	 * @param indexNames
	 *            索引list
	 * @param typeName
	 *            索引类型
	 * @param query
	 * @param pageSize
	 * @return
	 * @throws Exception
	 *             SearchResult
	 */
	public static SearchResult search(JestClient jestClient, List<String> indexNames, String typeName, String query, Integer pageSize) throws Exception {
		Search search = new Search.Builder(query).addIndex(indexNames).addType(typeName).setParameter(Parameters.SCROLL, "1m").setParameter(Parameters.SIZE, pageSize).build();
		return jestClient.execute(search);
	}

	/**
		 * {搜索文档}
		 *
		 * @author: zqli3@iflytek.com
		 * @createTime: 2017年5月19日 下午5:08:17
		 * @history:
		 * @param jestClient
		 *            client
		 * @param indexNameArr
		 *            索引名称数组
		 * @param typeName
		 *            索引类型
		 * @param query
		 *            查询条件
		 * @return
		 * @throws Exception
		 *             SearchResult
		 */
		public static SearchResult search(JestClient jestClient, String[] indexNameArr, String typeName, String query, Integer pageSize) throws Exception {
			
			return search(jestClient, Arrays.asList(indexNameArr), typeName, query, pageSize);
	//		Search search = new Search.Builder(query).addIndex(Arrays.asList(indexNameArr)).addType(typeName).setParameter(Parameters.SCROLL, "1m").setParameter(Parameters.SIZE, pageSize)
	//				.build();
	//		return jestClient.execute(search);
		}

	/**
	 * {分页搜索文档，传入pageSize和currentPageNo}
	 *
	 * @author: yeliu3@iflytek.com
	 * @createTime: 2017年6月14日 下午3:29:14
	 * @history:
	 * @param jestClient 
	 * @param indexName 索引名称
	 * @param typeName 索引类型
	 * @param query 查询条件
	 * @param pageSize 每页条数
	 * @param currentPageNo 当前页
	 * @return
	 * @throws Exception
	 */
	public static SearchResult search(JestClient jestClient, String indexName, String typeName, String query, Integer pageSize, Integer currentPageNo) throws Exception {
		Search search = new Search.Builder(query).addIndex(indexName).addType(typeName).setParameter(Parameters.SIZE, pageSize).setParameter(Parameters.FROM, (currentPageNo - 1) * pageSize).build();
		return jestClient.execute(search);
	}

	/**
	 * {根据查询条件 对单独求和获取求和之后的值}
	 * 
	 * @param jestClient
	 *            客户端
	 * @param indexName
	 *            索引名
	 * @param typeName
	 *            类型名
	 * @param query
	 *            查询条件
	 * @param sum
	 *            求和字段
	 * @return
	 * @author: pyma@iflytek.com
	 * 
	 * @throws IOException
	 */
	public static Double getAggSum(JestClient jestClient, String indexName, String typeName, String query, String sum) throws IOException {
		Search search = new Search.Builder(query).addIndex(indexName).addType(typeName).setParameter(Parameters.SCROLL, "1m").build();
		SearchResult result = jestClient.execute(search);
		MetricAggregation metricAggregation = result.getAggregations();
		Double num = metricAggregation.getSumAggregation(sum).getSum();
		return num;
	}

	/**
	 * {查询总数}
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @param query
	 *            查询条件
	 * @return
	 * @throws Exception
	 *             Double
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:30:32
	 * @history:
	 */
	public static Double count(JestClient jestClient, String indexName, String typeName, String query) throws Exception {
		Count count = new Count.Builder().addIndex(indexName).addType(typeName).query(query).build();
		CountResult results = jestClient.execute(count);
		return results.getCount();
	}

	/**
	 * { Get文档 }
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @param id
	 *            主键
	 * @return
	 * @throws Exception
	 *             JestResult
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:32:00
	 * @history:
	 */
	public static JestResult get(JestClient jestClient, String indexName, String typeName, String id) throws Exception {
		Get get = new Get.Builder(indexName, id).type(typeName).build();
		return jestClient.execute(get);
	}

	/**
	 * {根据索引，类型 ，查询条件循环查询所有}
	 * 
	 * @param aClass
	 *            返回list的类型
	 * @param jestClient
	 *            客户端
	 * @param indexName
	 *            索引名
	 * @param type
	 *            类型
	 * @param query
	 *            查询
	 * @author: pyma@iflytek.com
	 * 
	 * @return
	 * @throws Exception
	 */
	public static <T> List<T> getResult(Class<T> aClass, JestClient jestClient, String indexName, String type, String query) throws Exception {
		JestResult result = JestESUtil.search(jestClient, indexName, type, query, 10000);
		return resultHandle(aClass, jestClient, result);
	}

	/**
	 * {根据索引，类型 ，查询条件循环查询所有}
	 *
	 * @author: zqli3@iflytek.com
	 * @createTime: 2017年5月22日 上午10:37:05
	 * @history:
	 * @param aClass
	 *            返回list的类型
	 * @param jestClient
	 *            客户端
	 * @param indexNameArr
	 *            索引名数组
	 * @param type
	 *            类型
	 * @param query
	 *            查询
	 * @return
	 * @throws Exception
	 *             List<T>
	 */
	public static <T> List<T> getResult(Class<T> aClass, JestClient jestClient, String[] indexNameArr, String type, String query) throws Exception {
		JestResult result = JestESUtil.search(jestClient, indexNameArr, type, query, 10000);
		return resultHandle(aClass, jestClient, result);
	}

	/**
	 * 分页查询结果
	 *
	 * @author: pyma@iflytek.com
	 * @createTime: 2017年6月23日 上午9:13:32
	 * @history:
	 * @param aClass
	 * @param jestClient
	 *            客户端
	 * @param indexNames
	 *            索引list
	 * @param type
	 * @param query
	 * @return
	 * @throws Exception
	 *             List<T>
	 */
	public static <T> List<T> getResult(Class<T> aClass, JestClient jestClient, List<String> indexNames, String type, String query) throws Exception {
		JestResult result = JestESUtil.search(jestClient, indexNames, type, query, 10000);
		return resultHandle(aClass, jestClient, result);
	}

	/**
	 * {JestResult结果集封装，循环查询所有}
	 * 
	 * @author: zqli3@iflytek.com
	 * @createTime: 2017年5月22日 上午11:34:19
	 * @history:
	 * @param aClass
	 *            返回list的类型
	 * @param jestClient
	 *            客户端
	 * @param result
	 *            JestResult
	 * @return
	 * @throws IOException
	 *             List<T> 所有满足条件的结果
	 */
	private static <T> List<T> resultHandle(Class<T> aClass, JestClient jestClient, JestResult result) throws IOException {
		List<T> resultList = new ArrayList<T>();
		resultList = result.getSourceAsObjectList(aClass);
		String scrollId = result.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString();
		while (true) {
			if (null == scrollId || result.getSourceAsObjectList(aClass).size() == 0) {
				break;
			}
			SearchScroll scroll = new SearchScroll.Builder(scrollId, "1m").build();
			result = jestClient.execute(scroll);
			resultList.addAll(result.getSourceAsObjectList(aClass));
			scrollId = result.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString();
		}
		return resultList;
	}

	/**
	 * {根据id,更新记录}
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月16日 下午3:13:16
	 * @history:
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @param script
	 *            更新的json数据
	 * @param id
	 *            主键
	 * @return boolean
	 */
	public static boolean updateByScript(JestClient jestClient, String indexName, String typeName, Object script, String id) throws Exception {
		JestResult jr = jestClient.execute(new Update.Builder(script).index(indexName).type(typeName).id(id).build());
		return jr.isSucceeded();
	}

	/**
	 * 根据id更新对象
	 *
	 * @author: pyma@iflytek.com
	 * @createTime: 2017年6月15日 上午9:13:28
	 * @history:
	 * @param jestClient
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            类型名称
	 * @param source
	 *            更新的数据
	 * @param id
	 *            id
	 * @return
	 * @throws IOException
	 *             Boolean
	 */
	public static Boolean updateObjectById(JestClient jestClient, String indexName, String typeName, String source, String id) throws IOException {
		boolean result = jestClient.execute(new Index.Builder(source).index(indexName).type(typeName).id(id).build()).isSucceeded();
		return result;
	}

	/**
	 * {删除索引}
	 *
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @return
	 * @throws Exception
	 *             boolean
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月15日 下午7:32:56
	 * @history:
	 */
	public static boolean deleteIndex(JestClient jestClient, String indexName) throws Exception {
		JestResult jr = jestClient.execute(new DeleteIndex.Builder(indexName).build());
		return jr.isSucceeded();
	}

	/**
	 * {根据id删除记录}
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月16日 上午11:26:59
	 * @history:
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @param id
	 *            主键
	 * @return boolean
	 */
	public static boolean deleteIndexById(JestClient jestClient, String indexName, String typeName, String id) throws Exception {
		JestResult jr = jestClient.execute(new Delete.Builder(id).index(indexName).type(typeName).build());
		return jr.isSucceeded();
	}

	/**
	 * 根据查询条件进行删除
	 *
	 * @author: pyma@iflytek.com
	 * @createTime: 2017年7月31日 上午9:22:56
	 * @history:
	 * @param jestClient
	 *            客户端
	 * @param indexName
	 *            索引
	 * @param typeName
	 *            类型
	 * @param query
	 *            查询条件
	 * @return
	 * @throws Exception
	 *             boolean
	 */
	public static boolean deleteByQuery(JestClient jestClient, String indexName, String typeName, String query) throws Exception {
		DeleteByQuery deleteByQuery = new DeleteByQuery.Builder(query).addIndex(indexName).addType(typeName).build();
		JestResult result = jestClient.execute(deleteByQuery);
		return result.isSucceeded();
	}

	/**
	 * {判断索引是否存在,都存在-true}
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月18日 上午10:51:41
	 * @history:
	 * @param jestClient
	 *            client
	 * @param indexNames
	 *            索引名称(集合)
	 * @return
	 * @throws Exception
	 *             boolean
	 */
	public static boolean isExists(JestClient jestClient, List<String> indexNames) throws Exception {
		JestResult jr = jestClient.execute(new IndicesExists.Builder(indexNames).build());
		return jr.isSucceeded();
	}

	/**
	 * {判断索引是否存在,存在一个-true}
	 * @param jestClient
	 * @param indexNames 索引名称(集合)
	 * @return
	 * @throws Exception
	 */
	public static boolean isExistOne(JestClient jestClient, List<String> indexNames) throws Exception {
		for (String indexName : indexNames) {
			if (isExists(jestClient, indexName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * {判断索引是否存在,存在-true}
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月18日 上午10:51:41
	 * @history:
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @return
	 * @throws Exception
	 *             boolean
	 */
	public static boolean isExists(JestClient jestClient, String indexName) throws Exception {
		JestResult jr = jestClient.execute(new IndicesExists.Builder(indexName).build());
		return jr.isSucceeded();
	}

	/**
	 * {判断索引及类型是否存在,存在-true}
	 * 
	 * @author: kpchen@iflytek.com
	 * @createTime: 2017年5月18日 下午7:59:45
	 * @history:
	 * @param jestClient
	 *            client
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            索引类型
	 * @return
	 * @throws Exception
	 *             boolean
	 */
	public static boolean isExists(JestClient jestClient, String indexName, String typeName) throws Exception {
		JestResult jr = jestClient.execute(new TypeExist.Builder(indexName).addType(typeName).build());
		return jr.isSucceeded();
	}

	/**
	 * 根据id获取对象
	 *
	 * @author: pyma@iflytek.com
	 * @createTime: 2017年6月15日 上午9:12:47
	 * @history:
	 * @param jestClient
	 * @param indexName
	 *            索引名称
	 * @param typeName
	 *            类型名称
	 * @param id
	 *            id
	 * @param aClass
	 *            泛型
	 * @return
	 * @throws Exception
	 *             T
	 */
	public static <T> T getObjectById(JestClient jestClient, String indexName, String typeName, String id, Class<T> aClass) throws Exception {
		Get get = new Get.Builder(indexName, id).type(typeName).build();
		JestResult result = jestClient.execute(get);
		return result.getSourceAsObject(aClass);
	}

}
