package com.weidiango.bumblebee.codition;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.weidiango.bumblebee.tools.QueryParser;

/**
 * 基础的操作实体。
 *
 * @author ZongZi
 * @date 2019/4/26 5:22 PM
 */
public class QueryCondition {
    private String columnName;
    private SQLValuableExpr value;
    private SQLBinaryOperator operation;
    private Query query;

    public QueryCondition() {
    }


    public QueryCondition(String columnName, SQLValuableExpr value, SQLBinaryOperator operation) {
        this.columnName = columnName;
        this.value = value;
        this.operation = operation;
        this.query = QueryParser.parseQuery(this);
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public SQLValuableExpr getValue() {
        return value;
    }

    public void setValue(SQLValuableExpr value) {
        this.value = value;
    }

    public SQLBinaryOperator getOperation() {
        return operation;
    }

    public void setOperation(SQLBinaryOperator operation) {
        this.operation = operation;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    @Override
    public String toString() {
        return "QueryCondition{" +
                "columnName='" + columnName + '\'' +
                ", value=" + value +
                ", operation='" + operation + '\'' +
                '}';
    }
}
