package com.weidiango.componet.codition;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.fastjson.JSON;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.weidiango.componet.tools.QueryParser;

import java.util.List;

/**
 * @auther ZongZi
 * @date 2019/4/26 5:28 PM
 */
public class BoolQueryCondition extends QueryCondition {
    private List<QueryCondition> conditions;
    private SQLBinaryOperator operation;
    private BoolQuery query;

    public BoolQueryCondition(SQLBinaryOperator operation, List<QueryCondition> conditions) {
        super();
        this.conditions = conditions;
        this.operation = operation;
        this.query = QueryParser.generateBoolQuery(this);
    }

    @Override
    public BoolQuery getQuery() {
        return query;
    }

    public void setQuery(BoolQuery query) {
        this.query = query;
    }

    public List<QueryCondition> getConditions() {
        return conditions;
    }

    public void setConditions(List<QueryCondition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public SQLBinaryOperator getOperation() {
        return operation;
    }

    @Override
    public void setOperation(SQLBinaryOperator operation) {
        this.operation = operation;
    }

    @Override
    public String toString() {
        return "BoolQueryCondition{" +
                "conditions=" + JSON.toJSONString(conditions) +
                ", operation='" + operation + '\'' +
                '}';
    }
}
