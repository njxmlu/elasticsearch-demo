/*
 * 文件名：CellSheet
 * 版权：Copyright @ 2016 zhjt-web-ES-main All Rights Reserved.
 * 描述：城市网格小区信息
 * 修改人：tlhe@iflytek.com
 * 修改时间：2016年12月9日 下午2:10:32
 * 修改内容：〈修改内容〉
 */
package com.iflytek.es.jest;

import com.alibaba.fastjson.JSON;

/**
 * @desc: zhjt-web-ES-main
 * @author: tlhe@iflytek.com
 * @createTime: 2016年12月9日 下午2:10:32
 * @history:
 * @version: v1.0
 */
public class CellSheet {

	/**
	 * 小区id
	 */
	private String cellId;

	/**
	 * 小区中心点
	 */
	private double[] baiduLocation;

    /**
     * 扩样系数
     */
    private Double expansion;

	/**
	 * 小区覆盖半径
	 */
	private double radius;

    public Double getExpansion() {
        return expansion;
    }

    public void setExpansion(Double expansion) {
        this.expansion = expansion;
    }

    public String getCellId() {
		return cellId;
	}

	public void setCellId(String cellId) {
		this.cellId = cellId;
	}

	public double[] getBaiduLocation() {
		return baiduLocation;
	}

	public void setBaiduLocation(double[] baiduLocation) {
		this.baiduLocation = baiduLocation;
	}

	public double getRadius() {
		return radius;
	}

	public void setRadius(double radius) {
		this.radius = radius;
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

}
