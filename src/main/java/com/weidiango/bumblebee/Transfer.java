package com.weidiango.bumblebee;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.StringUtils;
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
    private static final String END_POINT = "https://reissue-Test.cn-hangzhou.ots.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "LTAIZejUplgu1n8c";
    private static final String ACCESS_KEY_SECRET = "yXdH1TK2o35it5ETp2KULn7syv82ZI";
    private static final String INSTANCE_NAME = "reissue-Test";
    private static final Integer MAX_OFF_SET = 2000;


    public static void main(String[] args) throws Exception {
        SyncClient client = new SyncClient(END_POINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET, INSTANCE_NAME);
        // for (int i = 0; i < 1000000; i++) {
        String sql = "SELECT toc.createTime,toc.logisticsFee,toc.mainPic,toc.mchId,toc.mpAlias,toc.name,toc.netflowAlias,toc.orderId,toc" +
                ".orderNumber,toc.orderStatus,toc.payAmount,toc.payNumber,toc.payPlatform,toc.phone,toc.productAlias,toc.productId,toc.productNum," +
                "toc.productPrice,toc.productPropKey,toc.productSKUAlias,toc.productSKUId,toc.productTitle,toc.pushStatus,toc.payType,toc" +
                ".suppliersId,toc.suppliersOrderId,toc.suppliersPackageHash,toc.totalPrice,toc.tradeId,toc.tradeNumber,toc.tradeStatus,toc.userId," +
                "toc.channelName,toc.mpUserRemark,toc.wxSettlementTotal FROM t_order_contract toc WHERE toc.netflowAlias in ('tjsp3Lk','rxarXgA'," +
                "'jecTlbW','UTduJvs','Er04RYi','WKQOTDg','AuEvGSl','BYC4aeg','E1MVKcI','6DHLZ6x','wthE8CE','pIDDGja','qokTSRI','qLKseX4','nvfkkCc'," +
                "'hV8zSLU','2bPJOMN','Tup7jva','S84dNMi','WN6708y','PSShWeZ','YHEbL8a','EotfIRn','sq146rT','qNQepoC','jj3Y165','WcEQI8p','ZRruxSK'," +
                "'2smmczC','lnKSXiP','wT2xoYS','6zfNLym','wc7kMNR','dJoQCud','bL5RsLS','rVCkVWX','eLnwEAs','NO_NETFLOW','BUmpRJT','bbrxSZa'," +
                "'q5UIQIc','Zwttr72','1Fso2G0','gBAIh6Q','CKt8vZh','lI84N7j','ePS0fk7','zAkMkU8','x4WEebu','UnQxEYO','xCfLXBE','aSMYyYg','2JDlvqS'," +
                "'lgRINz2','ozLgu6e','S7YlsxR','2akXCrM','AtADIRI','MWeh7jq','lesDTKo','wG7T62h','LufVmcp','DgNCCoA','c18CVUS','3i7hXJY','ObM7rmg'," +
                "'T2lGjoA','yTfM6N5','BDyRxFH','GpNacMc','GiXmrmJ','Ae5mdEd','6XdKxsJ','tddUbNM','vwrJWgG','InB3Fa8','vkW55Wv','sWH3Qnt','ZCslFcR'," +
                "'Kk7zkkG','KXwCs2K','nJzOVAs','mNbowlZ','IIWqOHt','Z1p74Ah','u8Hgf44','BUIc385','jqPCaA3','psgbYql','Mh1OjZe','uTyCjF2','QPnO4Nk'," +
                "'6DOPrBg','NHah2m5','VSzSKeK','tEl2v7c','0ZLeUWj','v1ZmZm5','PuygFa8','n7ticBD','qvWisa2','xjFq2jK','0YPvZVE','bg3jHin','opYFK1H'," +
                "'ry0PNSK','AYBQZzT','WcJqr5A','hcrGcTZ','5eKu0Uc','RvDAwwn','oksVo0F','6y73oV0','XfJhSiT','fX6dX3h','rLe5Y0V','LktfUjI','jWdDSDf'," +
                "'WkM7ldr','2hRpAcc','SBWUZUB','iGMF5fz','Yw86Yvx','NtSukJt','0xJKXuI','sd3BAYp','u7fzcNv','keNtnR6','pkZpTyK','dJXDl1T','i7xEDhc'," +
                "'smlUBOK','uvY1laB','gEKt8Az','AktAGw1','AYZnWq8','fMM21LV','VJfTKIa','51UaXZq','nUcvMIx','5MZcz4G','27YWOjB','i4TluJN','bQEn4lU'," +
                "'uIYb382','LynbkbM','Hxy5o8i','qIlH3wA','MFRCxb0','lDHRRJq','gSMWuvO','je1clut','YvzZlao','O1Q7Ilb','CRQzQxs','XIRYsfM','ixCj0qu'," +
                "'LTezCVD','maV8Hyw','tlktjNu','WyJ3Xij','yvHOw06','kCZD0Bm','jFhBDR1','81fHsWr','tYilPhS','4Wn8BQL','vBDM052','TgRFRJG','5CXrPTr'," +
                "'e6P8DSM','adi4SLJ','8M7Hqzr','DJM7jFX','dFesTTQ','vYwsbf7','gpJdItc','7lO67l7','UxeRUos','BVJwvS4','4paj0d6','iT6h8VC','8yXJxzC'," +
                "'ikh3HdY','SB5KwPQ','zL35X7M','m8ierR2','01fLNoM','gjbeH4E','NKeKv5F','HASmG7l','QPnvlND','Cm6pSWn','bOi7xfa','Y666lzw','aQ8KozR'," +
                "'VCjbbce','8lRgYNC','lLMQkzh','4SuONLz','IeR5zTd','vtga7eu','Zkn1bKF','ghc3bIY','zfdNzrm','eHMBY0D','CFrg3bC','GfaXAOm','QPnDNjM'," +
                "'tRQjuyb','TI3iG7j','o86Iodf','IW25bxl','MW6GuTV','a6ClELq','eg4bLfZ','ugLnNTk','CCmNW6f','PM8GF1x','cs8G82T','rQWUiu6','Z0Qf7LV'," +
                "'TtherxL','CjPhD23','PMGEYqo','dAnBT2Q','sgt6QoL','BhLGOpf','z8czOBL','l2c3Tvh','qOPPHTx','JSsPi8P','B6FPYQ6','xytT43W','oESNjLp'," +
                "'0vcyLUD','JzEeckc','K847jmJ','0tQT6ga','P7htxBi','nHRp7pG','AINBFGa','c52Ezah','whUdlIE','3Jwwb2w','fDQgViB','dgNPKWZ','ZeC1mMJ'," +
                "'34uVmho','DtN5IWJ','XA4naT7','OebjbCL','MdZyffd','oThmVxe','ry7SWwb','HhH3hgR','DZNUv6t','LOW0Wsq','suWUKRC','hsX2YwG','mJsIEiG'," +
                "'nbsXHuG','pQdAnG7','eMS1uEB','xmKz4ZK','DVVpA5s','VGsGgNi','xPWjglQ','CIDr65n','GopwIDs','CL0k2II','aoO3vmg','y7UyTAD','CoyUtSR'," +
                "'tObkUwW','1IgHqlP','dUI18E1','Iu2JkE7','u8aN1F1','QOeHx5Y','RhtaEBV','AazkrHi','g66SuuI','K3vyrvh','rWE5zKJ','vXPAUWg','qMwxPTp'," +
                "'dtxUoNu','2qtUjq5','DJqQzOl','CjuJEf4','N1lKa6z','ztexXyv','lZFQsxm','L4DincP','MASZTuA','qrnIQsZ','NVErtFq','RqpWsQa','gJKnIYk'," +
                "'1pNyx1L','4ywTyun','CfGsT2m','fvbsJAK','8CcWQl4','tAYHGGV','nVMoUJW','pDOaCwe','UTMcPBM','2CAxbrB','Oyds2JU','tKcCZhM','J3O250n'," +
                "'WMWdoZF','67BUuNs','iWh8nWc','yixhw4Q','mlZG6YU','gnwvjyx','NqpRjOO','g2nyjU0','7CTZYye','8x35L4o','NgNVmEH','zBMUNcb','tFRp5D2'," +
                "'rnRUDxU','ibyT87D','Crto8UR','WXTrCD4','AUGAHAo','6Yg7DpK','fuyUKQu','WsnfobA','e5cLkke','iWiueuZ','O3heZds','kat2jhg','CyfVW0F'," +
                "'gNdPzYR','wDv03nb','oU8erLy','W8VaaWC','dCsjlIe','85gweWf','oS7auQK','x8Lb60I','qgMfRE0','FR7QEcX','5PUroTF','x4BGDHI','OproW3y'," +
                "'6tLtQSi','tvO25H3','oStlOre','MWr4g0q','JPJvPRw','blD2A4m','FD3Eovu','Xhb61f5','gQuJKSR','tVC2EL3','KCs2qWI','URTudc8','r5N5njp'," +
                "'KiYKGHd','K80IUNx','ZD4zA6o','kPOrME8','ux2So0c','GNv5i6n','vvcy2Hn','bEkLXyG','WukyeU3','2K8Bsmy','3rUAIDn','XnCVdUX','noFZ65F'," +
                "'UqHPsyY','2fXjaui','mL5IrK4','JCWZ2qK','P4geH4b','cqahA8q','tjSZgAx','vrPZwsY','WNYkvPV','iAYiO2y','x5zuQJ2','EXtHQvp','A3Us3MK'," +
                "'0vdBjPH','aiFSg3B','qJOcXfV','qo2rdRH','NyKYvF8','7mvobFv','qMFxvgi','IrD8mM5','FUBVQ5f','wHC2F6U','hKts42q','4I5AF4R','1dDCa2e'," +
                "'b2p5fto','ezHwQtA','gbi6lM0','d2IFdVK','45Icx3q','Opm1iNV','6kEmvo6','rm6zlAK','lyVdBD7','Z6qg4Ph','HZvm5n4','3ynIoNS','N4u8A0f'," +
                "'yGCV4QA','6jkS4wC','7BhBZ1E','7h0ncd1','LGlVNt3','6Njj7e1','o1BlxLq','1QVBgoy','PnQv3DN','Tt8yC03','yzMmaEL','iH7Drxo','w8RzB55'," +
                "'udRSNvx','5dMZeJh','upwHBwn','vlWvm8n','xcBRPE5','SJopGR6','kxWnZJh','58Rd2qt','2DUJsjj','q3YuEaF','uJtTfw4','VOcb07C','8hzdYgg'," +
                "'NoPmfqN','xlaGFhx','y8OiRT8','W1GnMeA','BVIR4XM','lRzTgIY','40Ao1Xi','JCI68hJ','eozWOer','JC1kggl','zaIGSsS','k2e8IU7','mcWGcTy'," +
                "'P0NxiXq','3vc2NxF','LJpeiUE','l2nnLL5','lGFmDdH','uzJkbDU','IivwAhx','hjTDrSL','tdKXNql','CY1bsiM','OIhpjNp','7I7Fiol','bltD0nK'," +
                "'26x05yY','wmtsSkc','nsCIc5Q','gxAcvBM','UHiuJrp','dOczWG4','mJkTHfZ','glWKmF0','xio14Lw','5lgmofJ','FfSHkoU','u0yCMcH','bYNkvqK'," +
                "'pswbaVR','6lPJw42','Z27Wjzb','CcuwmOm','4a6Pv8v','gSYJKMn','ZvXNbax','rmmgdZ4','mvBQ4MA','XnPNO6y','hCgJlPQ','4AoA0DJ','D6gQfv8'," +
                "'jR3coQT','4waWm5a','uDerfXX','3XbPdkS','NLphsch','vW7RXt7','dwVjOKc','pEmJstE','oK0WPid','KzZm8DU','JpqLHbX','ROAaLMI','0UK61qk'," +
                "'aAH0OVt','AuZbO47','wIgESED','Xt3bYPA','IeICjyQ','JQpivTe','iZ8NMM2','RFIonwd','aJqsesg','uvYY5Ve','NagEKQ2','WiQZYum','KKlHSd5'," +
                "'MAZMw7B','s1pWEqe','Xfrg2f7','p8DjJTv','GQqxUjV','1OpHoL2','K7Wxofw','AOukByo','') and toc.productTitle like '2019欧美' and " +
                "toc.mpAlias='ghd8fa01bdf649' order by toc.score_field  DESC limit " +
                " 0,10";
        List<Row> transform = transform(client, "t_order_contract", "t_order_search", sql);
        for (int i = 0; i < transform.size(); i++) {
            Column[] columns = transform.get(i).getColumns();
            for (Column column : columns) {
                System.out.print(column.getName() + ":" + column.getValue() + ",");
            }
            System.out.println("");
        }
    }


    public static List<Row> transform(SyncClient syncClient, String tableName, String indexName, String sql) throws Exception {
        String dbType = JdbcConstants.MYSQL;
        SearchRequest searchRequest = new Transfer().sqlParser(sql, tableName, indexName, dbType);
        SearchResponse search = syncClient.search(searchRequest);
        return search.getRows();
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
