package com.weidiango.bumblebee;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;

import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.weidiango.bumblebee.codition.BoolQueryCondition;
import com.weidiango.bumblebee.codition.InQueryCondition;
import com.weidiango.bumblebee.codition.QueryCondition;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;


/**
 * @author ZongZi
 * @date 2019/3/27 8:16 PM
 */
public class Transfer {
    private static final String TABLE_NAME = "t_order_contract";
    private static final String END_POINT = "https://reissue-Test.cn-hangzhou.ots.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "LTAIZejUplgu1n8c";
    private static final String ACCESS_KEY_SECRET = "yXdH1TK2o35it5ETp2KULn7syv82ZI";
    private static final String INSTANCE_NAME = "reissue-Test";
    private static final Integer MAX_OFF_SET = 2000;
    private static final String INDEX_NAME = "t_order_search";


    public static void main(String[] args) throws Exception {
        SyncClient client = new SyncClient(END_POINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET, INSTANCE_NAME);
        // for (int i = 0; i < 1000000; i++) {
        String dbType = JdbcConstants.MYSQL;
        String sql = "select * from t_order_contract a where " +
                " a.tradeNumber like 'TR17*6A' and a.tradeId > 13271 limit " +
                "1,10 " +
                "order by a.tradeId desc";
        SearchRequest searchRequest = new Transfer().sqlParser(sql, dbType);
        SearchResponse search = client.search(searchRequest);
        List<Row> rows = search.getRows();
    }

    public SearchRequest sqlParser(String sql, String dbType) throws Exception {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        Assert.isTrue(stmtList.size() == 1);
        SQLStatement sqlStatement = stmtList.get(0);
        SQLSelectStatement selectStatement = (SQLSelectStatement) sqlStatement;
        SQLSelect statementSelect = selectStatement.getSelect();
        SearchQuery searchQuery = new SearchQuery();
        MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) statementSelect.getQuery();
        // set query condition
        generateQueryCondition(searchQuery, selectQueryBlock);
        // set sort
        generateSorts(searchQuery, selectQueryBlock);
        // set limits
        generateLimit(searchQuery, selectQueryBlock);
        // set lists
        return generateColumnsToGet(searchQuery, selectQueryBlock);
    }

    private void generateQueryCondition(SearchQuery searchQuery, MySqlSelectQueryBlock selectQueryBlock) throws Exception {
        Stack<Object> queryConditionStack = new Stack<>();
        SQLExpr whereExpr = selectQueryBlock.getWhere();
        if (whereExpr == null) {
            searchQuery.setQuery(null);
            return;
        }
        pushToStack(queryConditionStack, whereExpr);
        Assert.isTrue(queryConditionStack.size() <= 1);
        QueryCondition queryCondition = (QueryCondition) queryConditionStack.pop();
        // set query
        Query query = queryCondition.getQuery();
        searchQuery.setQuery(query);
    }

    private SearchRequest generateColumnsToGet(SearchQuery searchQuery, MySqlSelectQueryBlock selectQueryBlock) {
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
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
            sortList.add(new FieldSort(expr.getName(), order));
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
                    queryCondition = new BoolQueryCondition(queryCondition.getOperation(), Arrays.asList(queryCondition));
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
                    tableStoreInCondition = new BoolQueryCondition(tableStoreInCondition.getOperation(), Arrays.asList(tableStoreInCondition));
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
