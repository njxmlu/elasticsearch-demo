/*
* 文件名：JestESUtil
* 版权：Copyright @ 2017 zhjt-web All Rights Reserved.
* 描述：
* 修改人：kpchen@iflytek.com
* 修改时间：2017年5月15日 下午4:39:20
* 修改内容：〈修改内容〉
*/
package com.iflytek.es.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;

import java.util.List;

/**
 * {jest 方式操作ES}
 * 采用http协议
 *
 * @desc: zhjt-web
 * @author: kpchen@iflytek.com
 * @createTime: 2017年5月15日 下午4:39:20
 * @history:
 * @version: v1.0
 */
public class JestESUtil {

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
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://172.31.10.15:9200").multiThreaded(true).build());

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
	public void closeJestClient(JestClient jestClient) throws Exception {
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
	public boolean createIndex(JestClient jestClient, String indexName) throws Exception {
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
	public boolean createIndexMapping(JestClient jestClient, String indexName, String typeName, String source) throws Exception {
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
	public String getIndexMapping(JestClient jestClient, String indexName, String typeName) throws Exception {
		GetMapping getMapping = new GetMapping.Builder().addIndex(indexName).addType(typeName).build();
		JestResult jr = jestClient.execute(getMapping);
		return jr.getJsonString();
	}

	/**
	 * {索引文档  调用该接口的时候需要控制List<Object> objs 的数据量不要太大}
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
	public boolean index(JestClient jestClient, String indexName, String typeName, List<Object> objs) throws Exception {

		Bulk.Builder bulk = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName);
		for (Object obj : objs) {
			Index index = new Index.Builder(obj).build();
			bulk.addAction(index);
		}
		BulkResult br = jestClient.execute(bulk.build());
		return br.isSucceeded();
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
	public SearchResult search(JestClient jestClient, String indexName, String typeName, String query) throws Exception {

		Search search = new Search.Builder(query).addIndex(indexName).addType(typeName).build();
		return jestClient.execute(search);
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
	public Double count(JestClient jestClient, String indexName, String typeName, String query) throws Exception {

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
	public JestResult get(JestClient jestClient, String indexName, String typeName, String id) throws Exception {

		Get get = new Get.Builder(indexName, id).type(typeName).build();
		return jestClient.execute(get);
	}

	/**
	 * {根据id,根据记录}
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
	public boolean update(JestClient jestClient, String indexName, String typeName, Object script, String id) throws Exception {

		JestResult jr = jestClient.execute(new Update.Builder(script).index(indexName).type(typeName).id(id).build());

		return jr.isSucceeded();
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
	public boolean delete(JestClient jestClient, String indexName) throws Exception {

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
	public boolean delete(JestClient jestClient, String indexName, String typeName, String id) throws Exception {
		JestResult jr = jestClient.execute(new Delete.Builder(id).index(indexName).type(typeName).build());
		return jr.isSucceeded();
	}

}
