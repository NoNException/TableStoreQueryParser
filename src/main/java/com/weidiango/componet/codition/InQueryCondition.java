package com.weidiango.componet.codition;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.weidiango.componet.tools.QueryParser;

import java.util.List;

/**
 * @auther ZongZi
 * @date 2019/4/26 5:33 PM
 */
public class InQueryCondition extends QueryCondition {
    private String columnName;
    private Boolean isNotInList;
    private List<SQLExpr> listValues;
    private Query query;

    public InQueryCondition(String columnName, List<SQLExpr> listValues, Boolean operation) {
        this.columnName = columnName;
        this.listValues = listValues;
        this.isNotInList = operation;
        this.query = QueryParser.parseQuery(this);
    }

    public Boolean getNotInList() {
        return isNotInList;
    }

    public void setNotInList(Boolean notInList) {
        isNotInList = notInList;
    }

    @Override
    public Query getQuery() {
        return query;
    }

    @Override
    public void setQuery(Query query) {
        this.query = query;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public List<SQLExpr> getListValues() {
        return listValues;
    }

    public void setListValues(List<SQLExpr> listValues) {
        this.listValues = listValues;
    }

    @Override
    public String toString() {
        return "InQueryCondition{" +
                "columnName='" + columnName + '\'' +
                ", isNotInList=" + isNotInList +
                ", listValues=" + listValues +
                ", query=" + query +
                '}';
    }
}
