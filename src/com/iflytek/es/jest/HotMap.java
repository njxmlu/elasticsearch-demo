/*
* 文件名：HotMap
* 版权：Copyright @ 2017 zhjt-web All Rights Reserved.
* 描述：
* 修改人：kpchen@iflytek.com
* 修改时间：2017年5月15日 下午5:10:55
* 修改内容：〈修改内容〉
*/
package com.iflytek.es.jest;

import java.util.List;

import io.searchbox.annotations.JestId;

/**
 * @desc: zhjt-web
 * @author: kpchen@iflytek.com
 * @createTime: 2017年5月15日 下午5:10:55
 * @history:
 * @version: v1.0
 */
public class HotMap {
	/**
	 * 小区id
	 */
	@JestId
	private String cellId;
	/**
	 * 时间
	 */
	private String time;
	/**
	 * 该CI总人数
	 */
	private Integer userNum;

	/**
	 * 经纬度集合：第一个为经度，第二个为纬度
	 */
	private List<Double> baiduLocation;

	public HotMap() {
		super();
	}

	public HotMap(String cellId, Integer userNum) {
		super();
		this.cellId = cellId;
		this.userNum = userNum;
	}

	public HotMap(String time, String cellId, Integer userNum, List<Double> baiduLocation) {
		super();
		this.time = time;
		this.cellId = cellId;
		this.userNum = userNum;
		this.baiduLocation = baiduLocation;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getCellId() {
		return cellId;
	}

	public void setCellId(String cellId) {
		this.cellId = cellId;
	}

	public Integer getUserNum() {
		return userNum;
	}

	public void setUserNum(Integer userNum) {
		this.userNum = userNum;
	}

	public List<Double> getBaiduLocation() {
		return baiduLocation;
	}

	public void setBaiduLocation(List<Double> baiduLocation) {
		this.baiduLocation = baiduLocation;
	}

	@Override
	public String toString() {
		return "HotMap [time=" + time + ", cellId=" + cellId + ", userNum=" + userNum + ", baiduLocation=" + baiduLocation + "]";
	}

}
