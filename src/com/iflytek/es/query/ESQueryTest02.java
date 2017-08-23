/*
* Copyright @ 2016 com.iflysse.trains
* elasticsearch2.x 下午3:21:12
* All right reserved.
*
*/
package com.iflytek.es.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.iflytek.es.util.ESHandler;

/**
 * @desc: elasticsearch2.x
 * @author: kpchen@iflytek.com
 * @createTime: 2016年9月6日 下午3:21:12
 * @history:
 * @version: v1.0
 */
public class ESQueryTest02 {

	/**
	 * term 过滤器会查找我们设定的准确值<br>
	 * term 过滤器本身并不能起作用。像在【查询 DSL】中介绍的一样，搜索 API 需要得到一个 查询语句 ，而不是一个 过滤器 。<br>
	 * 为了使用 term 过滤器，我们需要将它包含在一个过滤查询语句中<br>
	 * 
	 * { "query" : { "filtered" : {"query" : { "match_all" : {}}, "filter" : { "term" : { "price" : 20 } } } } } <br>
	 */
	// 过滤器不会执行计分和计算相关性。分值由 match_all 查询产生，所有文档一视同仁，所有每个结果的分值都是 1
	@Test
	public void testFilterByTerm() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		// 过滤
		QueryBuilder postFilter = QueryBuilders.termQuery("account_number", 99);

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//

				.setQuery(queryBuilder)//
				.setPostFilter(postFilter)

				.execute().actionGet();

		SearchHits hits = searchResponse.getHits();
		System.out.println("查询到记录数=" + hits.getTotalHits());
		SearchHit[] searchHists = hits.getHits();
		if (searchHists.length > 0) {
			for (SearchHit hit : searchHists) {
				map = new HashMap<String, Object>();
				map.put("account_number", (Integer) hit.getSource().get("account_number"));
				map.put("balance", (Integer) hit.getSource().get("balance"));
				map.put("firstname", (String) hit.getSource().get("firstname"));
				map.put("lastname", (String) hit.getSource().get("lastname"));
				map.put("age", (Integer) hit.getSource().get("age"));
				map.put("gender", (String) hit.getSource().get("gender"));
				map.put("address", (String) hit.getSource().get("address"));
				map.put("employer", (String) hit.getSource().get("employer"));
				map.put("email", (String) hit.getSource().get("email"));
				map.put("city", (String) hit.getSource().get("city"));
				map.put("state", (String) hit.getSource().get("state"));
				list.add(map);
			}
		}
		System.out.println("返回结果" + list.size());
		System.out.println(JSON.toJSONString(list));
	}


	/**
	 * 在用term过滤字符串时，需要主要，字符串在查出来都变成小写字母了<br>
	 * 为了避免这种情况发生，我们需要通过设置这个字段为 not_analyzed 来告诉 Elasticsearch 它包含一个准确值。我们曾在
	 * 【自定义字段映射】中见过它。为了实现目标，我们要先删除旧索引（因为它包含了错误的映射），并创建一个正确映射的
	 * 
	 * <br>
	 * { "mappings" : { "products" : { "properties" : { "productID" : { "type" : "string", "index" : "not_analyzed" <3>
	 * } } } } }
	 * 
	 */
	// 过滤文本
	@Test
	public void testFilterByTerm02() {
		String indexname = "bank";
		String type = "account";

		// 过滤
		QueryBuilder postFilter = QueryBuilders.termQuery("firstname", "ratliff");

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setPostFilter(postFilter)

				.execute().actionGet();

		SearchHits hits = searchResponse.getHits();
		System.out.println("查询到记录数=" + hits.getTotalHits());
		SearchHit[] searchHists = hits.getHits();
		if (searchHists.length > 0) {
			for (SearchHit hit : searchHists) {
				map = new HashMap<String, Object>();
				map.put("account_number", (Integer) hit.getSource().get("account_number"));
				map.put("balance", (Integer) hit.getSource().get("balance"));
				map.put("firstname", (String) hit.getSource().get("firstname"));
				map.put("lastname", (String) hit.getSource().get("lastname"));
				map.put("age", (Integer) hit.getSource().get("age"));
				map.put("gender", (String) hit.getSource().get("gender"));
				map.put("address", (String) hit.getSource().get("address"));
				map.put("employer", (String) hit.getSource().get("employer"));
				map.put("email", (String) hit.getSource().get("email"));
				map.put("city", (String) hit.getSource().get("city"));
				map.put("state", (String) hit.getSource().get("state"));
				list.add(map);
			}
		}
		System.out.println("返回结果" + list.size());
		System.out.println(JSON.toJSONString(list));
	}

	// 布尔过滤器
	/**
	 * must ：所有分句都必须匹配，与 AND 相同。 must_not ：所有分句都必须不匹配，与 NOT 相同。 should ：至少有一个分句匹配，与 OR 相同。
	 * 
	 */
	@Test
	public void testBoolFilter() {
		String indexname = "bank";
		String type = "account";

		// 过滤
		QueryBuilder postFilter = QueryBuilders.boolQuery()
				//
				.should(QueryBuilders.termQuery("balance", 47159))//
				.should(QueryBuilders.termQuery("account_number", 98))//
				.mustNot(QueryBuilders.termQuery("age", 30));

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//

				.setPostFilter(postFilter)//

				.execute().actionGet();

		SearchHits hits = searchResponse.getHits();
		System.out.println("查询到记录数=" + hits.getTotalHits());
		SearchHit[] searchHists = hits.getHits();
		if (searchHists.length > 0) {
			for (SearchHit hit : searchHists) {
				map = new HashMap<String, Object>();
				map.put("account_number", (Integer) hit.getSource().get("account_number"));
				map.put("balance", (Integer) hit.getSource().get("balance"));
				map.put("firstname", (String) hit.getSource().get("firstname"));
				map.put("lastname", (String) hit.getSource().get("lastname"));
				map.put("age", (Integer) hit.getSource().get("age"));
				map.put("gender", (String) hit.getSource().get("gender"));
				map.put("address", (String) hit.getSource().get("address"));
				map.put("employer", (String) hit.getSource().get("employer"));
				map.put("email", (String) hit.getSource().get("email"));
				map.put("city", (String) hit.getSource().get("city"));
				map.put("state", (String) hit.getSource().get("state"));
				list.add(map);
			}
		}
		System.out.println("返回结果" + list.size());
		System.out.println(JSON.toJSONString(list));
	}

}
