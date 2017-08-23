/*
* 文件名：App
* 版权：Copyright @ 2017 zhjt-web All Rights Reserved.
* 描述：
* 修改人：kpchen@iflytek.com
* 修改时间：2017年5月15日 下午7:20:41
* 修改内容：〈修改内容〉
*/
package com.iflytek.es.jest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.fastjson.JSON;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchResult.Hit;

public class App {

	private static String indexName = "test-kpchen";
	private static String typeName = "test";
	static JestESUtil util = null;
	static JestClient jestClient = null;

	static {
		util = new JestESUtil();
		jestClient = util.getJestClient();
	}

	public static void main(String[] args) {
		try {// TODO
			App app = new App();
			app.groupby();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void createIndex() throws Exception {

		boolean result = util.createIndex(jestClient, indexName);
		System.out.println(result);
	}

	public void createIndexMapping() throws Exception {

		String source = "{\"" + typeName + "\":{\"properties\":{" + "\"id\":{\"type\":\"integer\"}" + ",\"name\":{\"type\":\"string\",\"index\":\"not_analyzed\"}" + ",\"birth\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'hh:mm:ss\"}"
				+ "}}}";
		System.out.println(source);
		boolean result = util.createIndexMapping(jestClient, indexName, typeName, source);
		System.out.println(result);
	}

	public void getIndexMapping() throws Exception {

		String result = util.getIndexMapping(jestClient, indexName, typeName);
		System.out.println(result);
	}

	public void index() throws Exception {

		List<Object> objs = new ArrayList<Object>();
		objs.add(new HotMap("hello java1", 205));
		// for (int i = 0; i < 10000; i++) {
		// objs.add(new HotMap("hello java" + i, 10 + i));
		// objs.add(new HotMap("hadoop" + i, 20 + i));
		// objs.add(new HotMap("T:o\"m-" + i, 33 + i));
		// objs.add(new HotMap("J,e{r}r;y:" + i, 55 + i));
		// }
		boolean result = util.index(jestClient, indexName, typeName, objs);
		System.out.println(result);
	}

	// 边界 过滤经纬度
	public void GeoBounds() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		// 定义一个集合存放边界经纬度
		List<Double[]> lonLats = new ArrayList<Double[]>();
		// String str =
		// "[[118.38578365270999,31.336491884115],[118.3870761683,31.335385794199],[118.38511473224,31.334617639357997],[118.38296270529999,31.334795985834997],[118.38145362002999,31.335248364479998],[118.38149875009,31.336480291086005],[118.38357715841,31.336790732024]]";
		String str = "[[118.456851,31.42011],[118.479848,31.410742],[118.461451,31.394468],[118.436154,31.412221]]";
		lonLats = JSON.parseArray(str, Double[].class);

		GeoPolygonQueryBuilder geoQb = QueryBuilders.geoPolygonQuery("baiduLocation");
		for (Double[] point : lonLats) {
			geoQb.addPoint(point[1], point[0]);
			System.out.println(point[1] + "," + point[0]);
		}

		searchSourceBuilder.query(geoQb);
		searchSourceBuilder.size(1000);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, "wuhu-cellsheet", "wuhu", query);
		List<Hit<CellSheet, Void>> hits = result.getHits(CellSheet.class);
		System.out.println("Size:" + hits.size());
		Set<String> set = new HashSet<String>();
		for (Hit<CellSheet, Void> hit : hits) {
			CellSheet user = hit.source;
			set.add(user.getCellId());
			System.out.println(user.toString());
		}
		System.out.println("去重 Size:" + set.size());
	}

	// 中心点+距离 过滤经纬度
	public void GetGeoDistance() throws Exception {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		// 定义一个中心点+半径
		double lon = 118.411207571723;
		double lat = 31.3958721205504;
		double distance = 1;

		GeoDistanceQueryBuilder disQb = QueryBuilders.geoDistanceQuery("baiduLocation");
		disQb.point(lat, lon).distance(distance, DistanceUnit.KILOMETERS).optimizeBbox("memory").geoDistance(GeoDistance.ARC);

		searchSourceBuilder.query(disQb);
		searchSourceBuilder.size(1000);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, "wuhu-cellsheet", "wuhu", query);
		List<Hit<CellSheet, Void>> hits = result.getHits(CellSheet.class);
		System.out.println("Size:" + hits.size());
		Set<String> set = new HashSet<String>();
		for (Hit<CellSheet, Void> hit : hits) {
			CellSheet user = hit.source;
			set.add(user.getCellId());
			System.out.println(user.toString());
		}
		System.out.println("去重 Size:" + set.size());
	}

	public void queryAll() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.size(10000); // 最大值可以通过查询条数来控制
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
		System.out.println("Size:" + hits.size());
		// TODO 自动映射为list
		List<HotMap> ts = result.getSourceAsObjectList(HotMap.class);
		System.out.println("Size:" + ts.size());
		for (Hit<HotMap, Void> hit : hits) {
			HotMap user = hit.source;
			System.out.println(user.toString());
		}
	}

	@SuppressWarnings("deprecation")
	public void termQuery() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder = QueryBuilders.termQuery("cellId", "hadoop23");// 单值完全匹配查询
		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.size(10);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		Integer total = result.getTotal();
		System.out.println(total);
		List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
		System.out.println("Size:" + hits.size());
		// TODO 自动映射为list
		List<HotMap> ts = result.getSourceAsObjectList(HotMap.class);
		System.out.println(JSON.toJSONString(ts));
		for (Hit<HotMap, Void> hit : hits) {
			HotMap user = hit.source;
			System.out.println(user.toString());
		}
	}

	public void termsQuery() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder = QueryBuilders.termsQuery("cellId", new String[] { "T:o\"m-", "J,e{r}r;y:" });// 多值完全匹配查询
		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.size(10);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
		System.out.println("Size:" + hits.size());
		for (Hit<HotMap, Void> hit : hits) {
			HotMap user = hit.source;
			System.out.println(user.toString());
		}
	}

	public void wildcardQuery() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("cellId", "*:*");// 通配符和正则表达式查询
		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.size(10);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
		System.out.println("Size:" + hits.size());
		for (Hit<HotMap, Void> hit : hits) {
			HotMap user = hit.source;
			System.out.println(user.toString());
		}
	}

	public void prefixQuery() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder = QueryBuilders.prefixQuery("cellId", "T:o");// 前缀查询
		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.size(10);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
		System.out.println("Size:" + hits.size());
		for (Hit<HotMap, Void> hit : hits) {
			HotMap user = hit.source;
			System.out.println(user.toString());
		}
	}

	public void rangeQuery() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder = QueryBuilders.rangeQuery("userNum").gte(10).lte(40).includeLower(true).includeUpper(true);// 区间查询
		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.size(10);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
		System.out.println("Size:" + hits.size());
		for (Hit<HotMap, Void> hit : hits) {
			HotMap user = hit.source;
			System.out.println(user.toString());
		}
	}

	public void queryString() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder = QueryBuilders.queryStringQuery(QueryParser.escape("T:o\""));// 文本检索，应该是将查询的词先分成词库中存在的词，然后分别去检索，存在任一存在的词即返回，查询词分词后是OR的关系。需要转义特殊字符
		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.size(10);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
		System.out.println("Size:" + hits.size());
		for (Hit<HotMap, Void> hit : hits) {
			HotMap user = hit.source;
			System.out.println(user.toString());
		}
	}

	public void filterSearch() throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		// 查询条件
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		// 过滤
		QueryBuilder postFilter = QueryBuilders.termQuery("cellId", "hello java8");

		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.postFilter(postFilter);
		searchSourceBuilder.size(10);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		Integer total = result.getTotal();
		System.out.println("total:" + total);
		if (null != total && total > 0) {
			List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
			System.out.println("Size:" + hits.size());
			for (Hit<HotMap, Void> hit : hits) {
				HotMap user = hit.source;
				System.out.println(user.toString());
			}
		}
	}

	// bool 过滤
	public void boolFilter() throws Exception {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		// 过滤
		QueryBuilder postFilter = QueryBuilders.boolQuery()
				//
				.should(QueryBuilders.termQuery("cellId", "J,e{r}r;y:6"))//
				.should(QueryBuilders.termQuery("cellId", "hello java14"))//
				.mustNot(QueryBuilders.termQuery("userNum", 240));

		searchSourceBuilder.postFilter(postFilter);
		searchSourceBuilder.size(100);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		Integer total = result.getTotal();
		System.out.println("total:" + total);
		if (null != total && total > 0) {
			List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
			System.out.println("Size:" + hits.size());
			for (Hit<HotMap, Void> hit : hits) {
				HotMap user = hit.source;
				System.out.println(user.toString());
			}
		}

	}

	// 分组查询
	public void groupby() throws Exception {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		// 分组 在aggs中设置size=0表示获取全部分组记录，等于其他值表示获取多少条
		AbstractAggregationBuilder aggregation = AggregationBuilders.terms("group_by_userNum").field("userNum").size(0);
		// 过滤
		QueryBuilder postFilter = QueryBuilders.termQuery("userNum", 205);
		searchSourceBuilder.postFilter(postFilter);
		searchSourceBuilder.aggregation(aggregation);
		searchSourceBuilder.size(100);
		searchSourceBuilder.from(0);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		SearchResult result = util.search(jestClient, indexName, typeName, query);
		Integer total = result.getTotal();
		System.out.println("total:" + total);
		if (null != total && total > 0) {
			List<Hit<HotMap, Void>> hits = result.getHits(HotMap.class);
			System.out.println("Size:" + hits.size());
			for (Hit<HotMap, Void> hit : hits) {
				HotMap user = hit.source;
				System.out.println(user.toString());
			}
		}
	}

	// 求和
	public void count() throws Exception {

		String[] name = new String[] { "T:o\"m-", "Jerry" };
		int from = 33;
		int to = 56;
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("cellId", name)).must(QueryBuilders.rangeQuery("userNum").gte(from).lte(to));
		searchSourceBuilder.query(queryBuilder);
		String query = searchSourceBuilder.toString();
		System.out.println(query);
		Double count = util.count(jestClient, indexName, typeName, query);
		System.out.println("Count:" + count);
	}

	public void get() throws Exception {

		String id = "hello java";
		JestResult result = util.get(jestClient, indexName, typeName, id);
		if (result.isSucceeded()) {
			HotMap user = result.getSourceAsObject(HotMap.class);
			System.out.println(user.toString());
		}
	}

	public void update() throws Exception {

		String script = "{\n" + "    \"script\" : \"ctx._source.tags += tag\",\n" + "    \"params\" : {\n" + "        \"tag\" : \"blue\"\n" + "    }\n" + "}";

		// String script = JSON.toJSONString(new HotMap("test sdsfsf", 100));
		String id = "hello java";
		boolean result = util.update(jestClient, indexName, typeName, script, id);
		System.out.println(result);

	}

	public void deleteIndexDocument() throws Exception {

		String id = "hello java";
		boolean result = util.delete(jestClient, indexName, typeName, id);
		System.out.println(result);
	}

	public void deleteIndex() throws Exception {

		boolean result = util.delete(jestClient, indexName);
		System.out.println(result);
	}

	public void deleteDocByid() throws Exception {

		boolean result = util.delete(jestClient, indexName, typeName, "AVwQAEgjPCuJY4LIghm_");
		System.out.println(result);
	}

}
