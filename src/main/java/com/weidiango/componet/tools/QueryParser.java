package com.weidiango.componet.tools;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.weidiango.componet.codition.BoolQueryCondition;
import com.weidiango.componet.codition.InQueryCondition;
import com.weidiango.componet.codition.QueryCondition;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zongzi
 * @date 2019/4/26 5:24 PM
 */
public class QueryParser {
    public static Query parseQuery(QueryCondition queryCondition) {
        if (queryCondition instanceof BoolQueryCondition) {
            return generateBoolQuery((BoolQueryCondition) queryCondition);
        } else {
            return generateQuery(queryCondition);
        }
    }

    public static BoolQuery generateBoolQuery(BoolQueryCondition boolQueryCondition) {
        SQLBinaryOperator operation = boolQueryCondition.getOperation();
        List<QueryCondition> conditions = boolQueryCondition.getConditions();
        List<Query> queries = new ArrayList<>();
        for (QueryCondition queryCondition : conditions) {
            Query query = queryCondition.getQuery();
            queries.add(query);
        }
        BoolQuery boolQuery = new BoolQuery();
        if (operation == SQLBinaryOperator.BooleanAnd) {
            boolQuery.setMustQueries(queries);
        }
        if (operation == SQLBinaryOperator.BooleanOr) {
            boolQuery.setShouldQueries(queries);
        }
        return boolQuery;
    }


    public static Query generateQuery(QueryCondition queryCondition) {

        String columnName = queryCondition.getColumnName();
        if (queryCondition instanceof InQueryCondition) {
            TermsQuery termsQuery = new TermsQuery();
            termsQuery.setFieldName(columnName);
            List<SQLExpr> listValues = ((InQueryCondition) queryCondition).getListValues();
            ArrayList<ColumnValue> columnValues = new ArrayList<>();
            for (SQLExpr sqlExpr : listValues) {
                SQLValuableExpr sqlValuableExpr = (SQLValuableExpr) sqlExpr;
                Object value = sqlValuableExpr.getValue();
                SQLDataType sqlDataType = sqlValuableExpr.computeDataType();
                ColumnValue columnValue = columnTypeConvert(sqlDataType, value);
                columnValues.add(columnValue);
            }
            termsQuery.setTerms(columnValues);
            return termsQuery;
        } else {
            SQLBinaryOperator operation = queryCondition.getOperation();
            SQLValuableExpr value = queryCondition.getValue();
            return queryTypeConvert(operation, value, columnName);
        }
    }


    public static Query queryTypeConvert(SQLBinaryOperator sqlBinaryOperator, SQLValuableExpr sqlValuableExpr, String columnName) {
        ColumnValue columnValue = columnTypyConvert(sqlValuableExpr);
        if (sqlBinaryOperator == SQLBinaryOperator.Equality) {
            TermQuery termQuery = new TermQuery();
            termQuery.setFieldName(columnName);
            termQuery.setTerm(columnValue);
            return termQuery;
        }
        if (sqlBinaryOperator.isRelational()) {
            List<SQLBinaryOperator> rangeOperator = Arrays.asList(SQLBinaryOperator.LessThan, SQLBinaryOperator.LessThan, SQLBinaryOperator.GreaterThan,
                    SQLBinaryOperator.GreaterThanOrEqual);
            if (rangeOperator.contains(sqlBinaryOperator)) {
                RangeQuery rangeQuery = new RangeQuery();
                rangeQuery.setFieldName(columnName);
                switch (sqlBinaryOperator) {
                    case LessThanOrEqual:
                        rangeQuery.lessThanOrEqual(columnValue);
                        break;
                    case LessThan:
                        rangeQuery.lessThan(columnValue);
                        break;
                    case GreaterThan:
                        rangeQuery.greaterThan(columnValue);
                        break;
                    case GreaterThanOrEqual:
                        rangeQuery.greaterThanOrEqual(columnValue);
                        break;
                    default:
                        return null;
                }
                return rangeQuery;
            }
            if (sqlBinaryOperator == SQLBinaryOperator.Like) {
                WildcardQuery wildcardQuery = new WildcardQuery();
                wildcardQuery.setFieldName(columnName);
                String value = (String) sqlValuableExpr.getValue();
                String s = value.replaceAll("%", "*");
                wildcardQuery.setValue(s);
                return wildcardQuery;
            }
        }
        return null;
    }

    static ColumnValue columnTypyConvert(SQLValuableExpr sqlValueExpr) {
        if (sqlValueExpr instanceof SQLNullExpr) {
            return null;
        }
        return columnTypeConvert(sqlValueExpr.computeDataType(), sqlValueExpr.getValue());
    }

    static ColumnValue columnTypeConvert(SQLDataType sqlDataType, Object value) {
        String dbType = sqlDataType.getDbType();
        if (StringUtils.isEmpty(dbType)) {
            dbType = sqlDataType.getName().toUpperCase();
        }
        switch (dbType) {
            case SQLDataType.Constants.VARCHAR:
            case SQLDataType.Constants.CHAR:
            case SQLDataType.Constants.TEXT:
                return ColumnValue.fromString((String) value);
            case SQLDataType.Constants.INT:
            case SQLDataType.Constants.BIGINT:
            case SQLDataType.Constants.TINYINT:
            case SQLDataType.Constants.SMALLINT:
                String valueStr = String.valueOf(value);
                return ColumnValue.fromLong(Long.valueOf(valueStr));
            case SQLDataType.Constants.DECIMAL:
                return ColumnValue.fromDouble((Double) value);
            case SQLDataType.Constants.BOOLEAN:
                return ColumnValue.fromBoolean((Boolean) value);
            case SQLDataType.Constants.BYTEA:
                return ColumnValue.fromBinary((byte[]) value);
            default:
                return ColumnValue.fromString(value.toString());
        }
    }
}
