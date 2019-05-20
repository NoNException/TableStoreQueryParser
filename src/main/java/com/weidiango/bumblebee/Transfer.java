package com.weidiango.bumblebee;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSONObject;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;

import com.alicloud.openservices.tablestore.model.search.sort.ScoreSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.weidiango.bumblebee.codition.BoolQueryCondition;
import com.weidiango.bumblebee.codition.InQueryCondition;
import com.weidiango.bumblebee.codition.QueryCondition;
import org.apache.commons.collections.CollectionUtils;


import java.util.*;

import static java.lang.String.format;


/**
 * @author ZongZi
 * @date 2019/3/27 8:16 PM
 */
public class Transfer {
    private static final String END_POINT = "";
    private static final String ACCESS_KEY_ID = "";
    private static final String ACCESS_KEY_SECRET = "";
    private static final String INSTANCE_NAME = "-sale";
    private static final Integer MAX_OFF_SET = 2000;


    public static void main(String[] args) throws Exception {
        SyncClient client = new SyncClient(END_POINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET, INSTANCE_NAME);
        // for (int i = 0; i < 1000000; i++) {
        String sql = "select * from table a" ;
        SearchResponse transform = transform(client, "t_order_contract", "t_order_search", sql);
        List<Row> rows = transform.getRows();
        for (int i = 0; i < rows.size(); i++) {
            Column[] columns = rows.get(i).getColumns();
            for (Column column : columns) {
                System.out.print(column.getName() + ":" + column.getValue().getType() + ",{" + column.getValue() + "} |\n ");
            }
            System.out.println("");
        }
        System.out.println(transform.getTotalCount());
    }


    public static SearchResponse transform(SyncClient syncClient, String tableName, String indexName, String sql) throws Exception {
        String dbType = JdbcConstants.MYSQL;
        SearchRequest searchRequest = new Transfer().sqlParser(sql, tableName, indexName, dbType);
        SearchResponse search = syncClient.search(searchRequest);
        return search;
    }

    public SearchRequest sqlParser(String sql, String tableName, String indexName, String dbType) throws Exception {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        if (stmtList.size() != 1) {
            throw new IllegalArgumentException(format("error sql:{}", sql));
        }
        SQLStatement sqlStatement = stmtList.get(0);
        SQLSelectStatement selectStatement = (SQLSelectStatement) sqlStatement;
        SQLSelect statementSelect = selectStatement.getSelect();
        SearchQuery searchQuery = new SearchQuery();
        MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) statementSelect.getQuery();
        // set query condition
        generateQueryCondition(searchQuery, selectQueryBlock);
        // set total count
        searchQuery.setGetTotalCount(true);
        // set sort
        generateSorts(searchQuery, selectQueryBlock);
        // set limits
        generateLimit(searchQuery, selectQueryBlock);
        // set lists
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, searchQuery);
        return generateColumnsToGet(searchRequest, selectQueryBlock);
    }

    private void generateQueryCondition(SearchQuery searchQuery, MySqlSelectQueryBlock selectQueryBlock) {
        Stack<Object> queryConditionStack = new Stack<>();
        SQLExpr whereExpr = selectQueryBlock.getWhere();
        if (whereExpr == null) {
            searchQuery.setQuery(null);
            return;
        }
        pushToStack(queryConditionStack, whereExpr);
        if (queryConditionStack.size() > 1) {
            throw new IllegalArgumentException("error parse sql");
        }
        QueryCondition queryCondition = (QueryCondition) queryConditionStack.pop();
        // set query
        Query query = queryCondition.getQuery();
        searchQuery.setQuery(query);
    }

    private SearchRequest generateColumnsToGet(SearchRequest searchRequest, MySqlSelectQueryBlock selectQueryBlock) {
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        List<SQLSelectItem> selectList = selectQueryBlock.getSelectList();
        List<String> columns = new ArrayList<>();
        for (SQLSelectItem sqlSelectItem : selectList) {
            if (sqlSelectItem.getExpr() instanceof SQLAllColumnExpr) {
                columnsToGet.setReturnAll(true);
                break;
            }
            SQLPropertyExpr sqlSelectItem1 = (SQLPropertyExpr) sqlSelectItem.getExpr();
            String name = sqlSelectItem1.getName();
            columns.add(name);
        }
        if (!columnsToGet.isReturnAll() && CollectionUtils.isNotEmpty(columns)) {
            columnsToGet.setColumns(columns);
        }
        searchRequest.setColumnsToGet(columnsToGet);
        return searchRequest;
    }

    private void generateLimit(SearchQuery searchQuery, MySqlSelectQueryBlock selectQueryBlock) throws Exception {
        SQLLimit limit = selectQueryBlock.getLimit();
        if (limit != null) {
            SQLIntegerExpr offset = (SQLIntegerExpr) limit.getOffset();
            int offSet = offset.getNumber().intValue();
            int rowCount = ((SQLIntegerExpr) limit.getRowCount()).getNumber().intValue();
            if (offSet + rowCount >= MAX_OFF_SET) {
                throw new Exception("offset:" + offSet + ", limit:" + limit + " > 2000");
            }
            searchQuery.setOffset(offSet);
            searchQuery.setLimit(rowCount);
        }
    }

    private void generateSorts(SearchQuery searchQuery, MySqlSelectQueryBlock selectQueryBlock) {
        SQLOrderBy orderBy = selectQueryBlock.getOrderBy();
        if (orderBy == null) {
            return;
        }
        List<SQLSelectOrderByItem> items = orderBy.getItems();
        List<Sort.Sorter> sortList = new ArrayList<>();
        for (SQLSelectOrderByItem sqlSelectOrderByItem : items) {
            SQLPropertyExpr expr = (SQLPropertyExpr) sqlSelectOrderByItem.getExpr();
            SQLOrderingSpecification type = sqlSelectOrderByItem.getType();
            SortOrder order = SortOrder.ASC;
            if (type != null && type == SQLOrderingSpecification.DESC) {
                order = SortOrder.DESC;
            }
            if (StringUtils.equals(expr.getName(), "score_field")) {
                ScoreSort scoreSort = new ScoreSort();
                scoreSort.setOrder(order);
                sortList.add(scoreSort);
            } else {
                sortList.add(new FieldSort(expr.getName(), order));
            }
        }
        if (CollectionUtils.isNotEmpty(sortList)) {
            searchQuery.setSort(new Sort(sortList));
        }
    }

    private void pushToStack(Stack<Object> stack, SQLExpr expr) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) expr;
            SQLExpr leftExpr = binaryOpExpr.getLeft();
            pushToStack(stack, leftExpr);
            SQLExpr rightExpr = binaryOpExpr.getRight();
            pushToStack(stack, rightExpr);
            SQLBinaryOperator operator = binaryOpExpr.getOperator();
            if (CollectionUtils.isEmpty(stack)) {
                return;
            }
            Object preStackObj = stack.peek();
            if (preStackObj instanceof SQLBinaryOperator) {
                stack.push(operator);
            } else if (preStackObj instanceof SQLExpr) {
                SQLValuableExpr right = (SQLValuableExpr) stack.pop();
                SQLPropertyExpr left = (SQLPropertyExpr) stack.pop();
                List<SQLBinaryOperator> notOperator = Arrays.asList(SQLBinaryOperator.NotLike,
                        SQLBinaryOperator.NotEqual);
                QueryCondition queryCondition = new QueryCondition(left.getName(), right, operator);
                if (notOperator.contains(operator)) {
                    queryCondition = new BoolQueryCondition(queryCondition.getOperation(), Collections.singletonList(queryCondition));
                }
                stack.push(queryCondition);
            } else {
                QueryCondition rightCondition = (QueryCondition) stack.pop();
                QueryCondition leftCondition = (QueryCondition) stack.pop();
                BoolQueryCondition finalBoolCondition = combineBoolCondition(operator.getName(), rightCondition, leftCondition);
                if (finalBoolCondition == null) {
                    finalBoolCondition = new BoolQueryCondition(operator, Arrays.asList(rightCondition, leftCondition));
                }
                stack.push(finalBoolCondition);
            }
        } else {
            if (expr instanceof SQLInListExpr) {
                SQLInListExpr listValues = (SQLInListExpr) expr;
                SQLPropertyExpr propertyExpr = (SQLPropertyExpr) listValues.getExpr();
                List<SQLExpr> targetList = listValues.getTargetList();
                QueryCondition tableStoreInCondition = new InQueryCondition(propertyExpr.getName(), targetList, listValues.isNot());
                if (listValues.isNot()) {
                    tableStoreInCondition = new BoolQueryCondition(tableStoreInCondition.getOperation(), Collections.singletonList(tableStoreInCondition));
                }
                stack.push(tableStoreInCondition);
            } else {
                stack.push(expr);
            }
        }

    }

    private BoolQueryCondition combineBoolCondition(String currentOperator, QueryCondition... queryConditions) {
        List<QueryCondition> finalCondition = new ArrayList<>();
        SQLBinaryOperator finalOperation = null;
        for (QueryCondition queryCondition : queryConditions) {
            if (queryCondition instanceof BoolQueryCondition) {
                finalCondition.addAll(new ArrayList<>(((BoolQueryCondition) queryCondition).getConditions()));
                finalOperation = queryCondition.getOperation();
            } else {
                finalCondition.add(queryCondition);
            }
        }
        Boolean combineNewBoolCondition = finalOperation != null && StringUtils.equals(finalOperation.getName(), currentOperator);
        return combineNewBoolCondition ? new BoolQueryCondition(finalOperation, finalCondition) : null;
    }

}
