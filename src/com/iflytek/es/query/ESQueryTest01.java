/*
 * Copyright @ 2016 com.iflysse.trains
 * elasticsearch2.x 下午2:59:58
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
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.es.util.ESHandler;

/**
 * @desc: elasticsearch2.x
 * @author: kpchen@iflytek.com
 * @createTime: 2016年9月2日 下午2:59:58
 * @history:
 * @version: v1.0
 */
public class ESQueryTest01 {

	/**
	 * 【1】 { "query": { "match_all": {} } }
	 */
	@Test
	public void testMatchAll() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)
				.setQuery(queryBuilder).execute().actionGet();

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
	 * 【2】 { "query": { "match_all": {} }, "size": 1 }' <br/>
	 * 【3】{ "query": { "match_all": {} }, "from": 10, "size": 10 }
	 */
	@Test
	public void testMatchAllSize() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)
				.setQuery(queryBuilder).setSize(10).setFrom(10).execute().actionGet();

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
	 * 【4】{ "query": { "match_all": {} }, "sort": { "balance": { "order": "desc" } } }
	 */
	@Test
	public void testMatchAllSort() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder).addSort("balance", SortOrder.DESC) //
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
	 * TODO 有选择的输出某些字段<br>
	 * 【5】{ "query": { "match_all": {} }, "_source": ["account_number", "balance"] }
	 */
	@Test
	public void testMatchAllSource() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder)//
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
	 * 返回 account_number=20<br>
	 * 【6】{ "query": { "match": { "account_number": 20 } } }
	 */
	@Test
	public void testMatch() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchQuery("account_number", 20);

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder)//
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
	 * 返回 address=mill or address=lane： 支持字符串(包含)<br>
	 * 【7】{ "query": { "match": { "address": "mill lane" } } }
	 */
	@Test
	public void testMatchStr() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchQuery("address", "mill lane");

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder)//
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
	 * 返回 短语匹配 address=mill lane： 精确匹配到包含“mill lane”<br>
	 * 【8】{ "query": { "match_phrase": { "address": "mill lane" } } }
	 */
	@Test
	public void testMatchPhrase() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("address", "mill lane");

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder)//
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
	 * 返回 匹配address=mill & address=lane<br>
	 * 【9】{ "query": { "bool": { "must": [ { "match": { "address": "mill" } }, { "match": { "address": "lane" } } ] } }
	 * }
	 */
	@Test
	public void testBoolMust1() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.boolQuery()//
				.must(QueryBuilders.matchQuery("address", "mill")).must(QueryBuilders.matchQuery("address", "lane"));

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder)//
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
	 * 返回 匹配address=mill & address=lane<br>
	 * 【9】{ "query": { "bool": { "must": [ { "match": { "address": "mill" } }, { "match": { "address": "lane" } } ] } }
	 * }
	 */
	@Test
	public void testBoolMust() {
		String indexname = "bank";
		String type = "account";
	
		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.boolQuery()//
				.must(QueryBuilders.matchQuery("address", "mill")).must(QueryBuilders.matchQuery("address", "lane"));
	
		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder)//
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
	 * 返回 匹配address=mill or address=lane：<br>
	 * 【10】{ "query": { "bool": { "should": [ { "match": { "address": "mill" } }, { "match": { "address": "lane" } } ] }
	 * } } }
	 */
	@Test
	public void testBoolShould() {
		String indexname = "bank";
		String type = "account";

		// 查询条件 matchPhraseQuery:精确------- matchQuery：模糊，不区分大小写
		QueryBuilder queryBuilder = QueryBuilders.boolQuery()
				//
				.should(QueryBuilders.matchPhraseQuery("address", "mill"))
				.should(QueryBuilders.matchPhraseQuery("address", "lane"));

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder)//
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
	 * 返回 不匹配address=mill & address=lane：<br>
	 * 【11】{ "query": { "bool": { "must_not": [ { "match": { "address": "mill" } }, { "match": { "address": "lane" } } ]
	 * } } }
	 */
	@Test
	public void testBoolMustNot() {
		String indexname = "bank";
		String type = "account";

		// 查询条件 matchPhraseQuery:精确------- matchQuery：模糊，不区分大小写
		QueryBuilder queryBuilder = QueryBuilders.boolQuery()
				//
				.mustNot(QueryBuilders.matchPhraseQuery("address", "mill"))
				.mustNot(QueryBuilders.matchPhraseQuery("address", "lane"));

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(queryBuilder)//
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
	 * 返回 balance>=20000 && balance<=30000<br>
	 * 【13】{ "query": { "filtered": { "query": { "match_all": {} }, "filter": { "range": { "balance": { "gte": 20000,
	 * "lte": 30000 } } } } } }
	 */
	@Test
	public void testFilterRange() {
		String indexname = "bank";
		String type = "account";

		// 查询条件
		// QueryBuilder queryBuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), QueryBuilders
		// .rangeQuery("balance").gte(27714).lte(27715));

		// 或者setPostFilter

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(QueryBuilders.matchAllQuery())//
				.setPostFilter(QueryBuilders.rangeQuery("balance").gte(27714).lte(27715))//
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
	 * 返回根据state分组，降序统计top 10 state<br>
	 * 【14】{ "size": 0, "aggs": { "group_by_state": { "terms": { "field": "state" } } } } }
	 */
	@Test
	public void testGroupBy() {
		String indexname = "bank";
		String type = "account";

		// 分组 在aggs中设置size=0表示获取全部分组记录，等于其他值表示获取多少条
		AbstractAggregationBuilder aggregation = AggregationBuilders.terms("group_by_state").field("state").size(0);

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(QueryBuilders.matchAllQuery())//
				.setSize(0)// 不取hist 记录，只获取分组信息
				.addAggregation(aggregation).execute().actionGet();

		// aggregations
		Aggregations aggs = searchResponse.getAggregations();
		Terms terms = (Terms) aggs.get("group_by_state");
		List<Bucket> aList = terms.getBuckets();

		if (aList.size() > 0) {
			for (Bucket b : aList) {
				map = new HashMap<String, Object>();
				map.put("key", b.getKey());
				map.put("count", b.getDocCount());
				list.add(map);
			}
		}
		System.out.println("返回结果" + list.size());
		System.out.println(JSON.toJSONString(list));
	}

	/**
	 * 根据state计算账户平均balance，降序统计top 10 state<br>
	 * 【15】{ "size": 0, "aggs": { "group_by_state": { "terms": { "field": "state" }, "aggs": { "average_balance": {
	 * "avg": { "field": "balance" } } } } } }
	 */
	@Test
	public void testAvgSumMaxMinGroupBy() {
		String indexname = "bank";
		String type = "account";

		// 分组 在aggs中设置size=0表示获取全部分组记录，等于其他值表示获取多少条
		AbstractAggregationBuilder aggregation = AggregationBuilders.terms("group_by_state").field("state")
				//
				.subAggregation(AggregationBuilders.avg("avg_balance").field("balance"))
				//
				.subAggregation(AggregationBuilders.sum("sum_balance").field("balance"))
				//
				.subAggregation(AggregationBuilders.max("max_balance").field("balance"))
				.subAggregation(AggregationBuilders.min("min_balance").field("balance")).size(0);

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(QueryBuilders.matchAllQuery())//
				.setSize(0)// 不取hist 记录，只获取分组信息
				.addAggregation(aggregation).execute().actionGet();

		// aggregations
		Aggregations aggs = searchResponse.getAggregations();
		Terms terms = (Terms) aggs.get("group_by_state");
		List<Bucket> aList = terms.getBuckets();

		if (aList.size() > 0) {
			for (Bucket b : aList) {
				map = new HashMap<String, Object>();
				map.put("key", b.getKey());
				map.put("count", b.getDocCount());
				Min minVar = b.getAggregations().get("min_balance");
				Max maxVar = b.getAggregations().get("max_balance");
				Avg avgVar = b.getAggregations().get("avg_balance");
				map.put("min", minVar.getValue());
				map.put("max", maxVar.getValue());
				map.put("avg", avgVar.getValue());

				list.add(map);
			}
		}
		System.out.println("返回结果" + list.size());
		System.out.println(JSON.toJSONString(list));
	}

	/**
	 * 以“state”分组，求“balance”平均值，按平均值降序输出<br>
	 * 【16】{ "size": 0, "aggs": { "group_by_state": { "terms": { "field": "state", "order": { "average_balance": "desc"
	 * } }, "aggs": { "average_balance": { "avg": { "field": "balance" } } } } } }
	 */
	@Test
	public void testAvgAndSortGroupBy() {
		String indexname = "bank";
		String type = "account";

		// 分组 在aggs中设置size=0表示获取全部分组记录，等于其他值表示获取多少条
		AbstractAggregationBuilder aggregation = AggregationBuilders.terms("group_by_state").field("state")
				.order(Order.aggregation("avg_balance", false))
				//
				.subAggregation(AggregationBuilders.avg("avg_balance").field("balance"))//
				.size(0);

		// 结果集
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = null;
		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(QueryBuilders.matchAllQuery())//
				.setSize(0)// 不取hist 记录，只获取分组信息
				.addAggregation(aggregation).execute().actionGet();

		// aggregations
		Aggregations aggs = searchResponse.getAggregations();
		Terms terms = (Terms) aggs.get("group_by_state");
		List<Bucket> aList = terms.getBuckets();

		if (aList.size() > 0) {
			for (Bucket b : aList) {
				map = new HashMap<String, Object>();
				map.put("key", b.getKey());
				map.put("count", b.getDocCount());
				Avg avgVar = b.getAggregations().get("avg_balance");
				map.put("avg", avgVar.getValue());

				list.add(map);
			}
		}
		System.out.println("返回结果" + list.size());
		System.out.println(JSON.toJSONString(list));
	}

	/**
	 * 聚合年龄分区间(ages 20-29, 30-39, and 40-49),聚合性别，最后平均balance 展示最终结果<br>
	 * 【17】{ "size": 0, "aggs": { "group_by_age": { "range": { "field": "age", "ranges": [ { "from": 20, "to": 30 }, {
	 * "from": 30, "to": 40 }, { "from": 40, "to": 50 } ] }, "aggs": { "group_by_gender": { "terms": { "field": "gender"
	 * }, "aggs": { "average_balance": { "avg": { "field": "balance" } } } } } } } }
	 */
	// 利用fastjosn工具类来解析json字符串
	@Test
	public void testAggGroupBy() {
		String indexname = "bank";
		String type = "account";

		// 分组 在aggs中设置size=0表示获取全部分组记录，等于其他值表示获取多少条
		AbstractAggregationBuilder aggregation = AggregationBuilders
				.range("group_by_age")
				.field("age")
				.addRange(20, 30)
				.addRange(30, 40)
				.addRange(40, 50)
				.subAggregation(
						AggregationBuilders.terms("group_by_gender").field("gender")
								.subAggregation(AggregationBuilders.avg("average_balance").field("balance")).size(0)

				);

		SearchResponse searchResponse = ESHandler.getTransportClient().prepareSearch(indexname).setTypes(type)//
				.setQuery(QueryBuilders.matchAllQuery())//
				.setSize(0)// 不取hist 记录，只获取分组信息
				.addAggregation(aggregation).execute().actionGet();

		// System.out.println(searchResponse.toString());
		String temp = searchResponse.toString();
		JSONObject obj = JSONObject.parseObject(temp);
		// System.out.println(obj.get("took"));
		// System.out.println(obj.getJSONObject("aggregations").getJSONObject("group_by_age").getJSONArray("buckets"));
		JSONArray arr = obj.getJSONObject("aggregations").getJSONObject("group_by_age").getJSONArray("buckets");
		for (int i = 0; i < arr.size(); i++) {
			JSONObject o = arr.getJSONObject(i);
			System.out.println(o.get("key"));
			System.out.println(o.get("doc_count"));
			System.out.println(o.getJSONObject("group_by_gender").getJSONArray("buckets").getJSONObject(0)
					.getJSONObject("average_balance").get("value"));
			break;
		}
	}

}
