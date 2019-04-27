package com.weidiango.bumblebee.tools;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.weidiango.bumblebee.codition.BoolQueryCondition;
import com.weidiango.bumblebee.codition.InQueryCondition;
import com.weidiango.bumblebee.codition.QueryCondition;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.weidiango.bumblebee.tools.QueryParser.LikeQuery.computeQuery;

/**
 * 转换器，根据SQL解析的结果，确定数据类型和数据的值，补充生成tableStore的query
 *
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
        List<Query> mustNotQueries = new ArrayList<>();
        for (QueryCondition queryCondition : conditions) {
            Query query = queryCondition.getQuery();
            if (queryCondition instanceof InQueryCondition && ((InQueryCondition) queryCondition).getNotInList()) {
                TermsQuery termsQuery = (TermsQuery) queryCondition.getQuery();
                mustNotQueries.add(termsQuery);
                continue;
            }
            queries.add(query);
        }
        BoolQuery boolQuery = new BoolQuery();
        if (operation == SQLBinaryOperator.BooleanAnd) {
            boolQuery.setMustQueries(queries);
        }
        if (operation == SQLBinaryOperator.BooleanOr) {
            boolQuery.setShouldQueries(queries);
        }
        List<SQLBinaryOperator> notOperator = Arrays.asList(SQLBinaryOperator.NotLike,
                SQLBinaryOperator.NotEqual);
        if (notOperator.contains(operation)) {
            boolQuery.setMustNotQueries(queries);
        }
        if (CollectionUtils.isNotEmpty(mustNotQueries)) {
            List<Query> mustNotQueries1 = boolQuery.getMustNotQueries();
            if (CollectionUtils.isNotEmpty(mustNotQueries1)) {
                mustNotQueries.addAll(mustNotQueries1);
            }
            boolQuery.setMustNotQueries(mustNotQueries);
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
        ColumnValue columnValue = columnTypeConvert(sqlValuableExpr);
        if (sqlBinaryOperator == SQLBinaryOperator.Equality || sqlBinaryOperator == SQLBinaryOperator.NotEqual) {
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
            if (sqlBinaryOperator == SQLBinaryOperator.Like || sqlBinaryOperator == SQLBinaryOperator.NotLike) {
                String value = (String) sqlValuableExpr.getValue();
                return computeQuery(value, columnName);
            }
        }
        return null;
    }

    static ColumnValue columnTypeConvert(SQLValuableExpr sqlValueExpr) {
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


    /**
     * 模糊匹配的QUERY生成工具,
     * 如果匹配符（*|?|%|_）只要存在开头,或者没有匹配符,则用文本匹配
     * 如果匹配符只存在结尾，则使用前缀匹配
     * 其他情况使用通配符查询。
     * <p>
     * 如果查询的字段在tableStore的索引是支持分词的话，建议直接使用文本匹配的输入格式，其他两种方式会导致查询准确，
     * 如果是字符类型的话，则建议使用后两种方式
     */
    public enum LikeQuery {
        /**
         * 模糊匹配的转换
         */
        matchQueryStart("^(\\*|\\?).*$ "),
        matchQueryNone("^((?!\\*|\\?).)*$ "),
        prefixQuery("^((?!\\*|\\?).)*(\\*)");
        private String regex;


        LikeQuery(String regex) {
            this.regex = regex;
        }

        public static Boolean matchQueryType(LikeQuery likeQuery, String value) {
            String regex = likeQuery.getRegex();
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(value);
            return matcher.matches();
        }

        public static Query computeQuery(String value, String columnName) {

            if (StringUtils.isEmpty(value) || StringUtils.isEmpty(columnName)) {
                throw new IllegalArgumentException("value or columnName is null [" + value + "," + columnName + "]");
            }
            value = value.replaceAll("%", "*");
            value = value.replaceAll("_", "?");
            if (LikeQuery.matchQueryType(matchQueryStart, value) || LikeQuery.matchQueryType(matchQueryNone, value)) {
                MatchQuery matchQuery = new MatchQuery();
                value = value.replaceAll("([*?])", "");
                matchQuery.setText(value);
                matchQuery.setFieldName(columnName);
                return matchQuery;
            }

            if (LikeQuery.matchQueryType(prefixQuery, value)) {
                PrefixQuery prefixQuery = new PrefixQuery();
                value = value.replaceAll("([*?])", "");
                prefixQuery.setPrefix(value);
                prefixQuery.setFieldName(columnName);
                return prefixQuery;
            }

            WildcardQuery wildcardQuery = new WildcardQuery();
            wildcardQuery.setFieldName(columnName);
            wildcardQuery.setValue(value);
            return wildcardQuery;
        }

        public String getRegex() {
            return regex;
        }
    }
}
