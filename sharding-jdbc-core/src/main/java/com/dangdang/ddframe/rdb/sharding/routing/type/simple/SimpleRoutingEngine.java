/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.sharding.routing.type.simple;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataNode;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.hint.HintManagerHolder;
import com.dangdang.ddframe.rdb.sharding.hint.ShardingKey;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.condition.Column;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.condition.Condition;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatement;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 简单路由引擎.
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
@Slf4j
public final class SimpleRoutingEngine implements RoutingEngine {
    
    private final ShardingRule shardingRule;
    
    private final List<Object> parameters;
    
    private final String logicTableName;
    
    private final SQLStatement sqlStatement;
    
    @Override
    public RoutingResult route() {
        // 根据逻辑表得到tableRule，逻辑表为t_order；表规则的配置为：.actualTables(Arrays.asList("t_order_0", "t_order_1"))，所以有两个实际表；
        TableRule tableRule = shardingRule.getTableRule(logicTableName);
        // 根据规则先路由数据源：即根据user_id取模路由
        Collection<String> routedDataSources = routeDataSources(tableRule);
        // routedMap保存路由到的目标数据源和表的结果：key为数据源，value为该数据源下路由到的目标表集合
        Collection<String> routedTables = routeTables(tableRule, routedDataSources);
		// 将得到的路由数据源和表信息封装到RoutingResult中，RoutingResult中有个TableUnits类型属性，
        // TableUnits类中有个List<TableUnit> tableUnits属性，
        // TableUnit包含三个属性：dataSourceName--数据源名称，logicTableName--逻辑表名称，actualTableName--实际表名称，
        // 例如：TableUnit:{dataSourceName:ds_jdbc_1, logicTableName:t_order, actualTableName: t_order_1}
        return generateRoutingResult(tableRule, routedDataSources, routedTables);
    }
    
    private Collection<String> routeDataSources(final TableRule tableRule) {
        DatabaseShardingStrategy strategy = shardingRule.getDatabaseShardingStrategy(tableRule);
        List<ShardingValue<?>> shardingValues = HintManagerHolder.isUseShardingHint() ? getDatabaseShardingValuesFromHint(strategy.getShardingColumns())
                : getShardingValues(strategy.getShardingColumns());
        logBeforeRoute("database", logicTableName, tableRule.getActualDatasourceNames(), strategy.getShardingColumns(), shardingValues);
        Collection<String> result = strategy.doStaticSharding(sqlStatement.getType(), tableRule.getActualDatasourceNames(), shardingValues);
        logAfterRoute("database", logicTableName, result);
        Preconditions.checkState(!result.isEmpty(), "no database route info");
        return result;
    }
    
    private Collection<String> routeTables(final TableRule tableRule, final Collection<String> routedDataSources) {
        TableShardingStrategy strategy = shardingRule.getTableShardingStrategy(tableRule);
        List<ShardingValue<?>> shardingValues = HintManagerHolder.isUseShardingHint() ? getTableShardingValuesFromHint(strategy.getShardingColumns())
                : getShardingValues(strategy.getShardingColumns());
        logBeforeRoute("table", logicTableName, tableRule.getActualTables(), strategy.getShardingColumns(), shardingValues);
        Collection<String> result = tableRule.isDynamic() ? strategy.doDynamicSharding(shardingValues)
                : strategy.doStaticSharding(sqlStatement.getType(), tableRule.getActualTableNames(routedDataSources), shardingValues);
        logAfterRoute("table", logicTableName, result);
        Preconditions.checkState(!result.isEmpty(), "no table route info");
        return result;
    }
    
    private List<ShardingValue<?>> getDatabaseShardingValuesFromHint(final Collection<String> shardingColumns) {
        List<ShardingValue<?>> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<ShardingValue<?>> shardingValue = HintManagerHolder.getDatabaseShardingValue(new ShardingKey(logicTableName, each));
            if (shardingValue.isPresent()) {
                result.add(shardingValue.get());
            }
        }
        return result;
    }
    
    private List<ShardingValue<?>> getTableShardingValuesFromHint(final Collection<String> shardingColumns) {
        List<ShardingValue<?>> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<ShardingValue<?>> shardingValue = HintManagerHolder.getTableShardingValue(new ShardingKey(logicTableName, each));
            if (shardingValue.isPresent()) {
                result.add(shardingValue.get());
            }
        }
        return result;
    }
    
    private List<ShardingValue<?>> getShardingValues(final Collection<String> shardingColumns) {
        List<ShardingValue<?>> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<Condition> condition = sqlStatement.getConditions().find(new Column(each, logicTableName));
            if (condition.isPresent()) {
                result.add(condition.get().getShardingValue(parameters));
            }
        }
        return result;
    }
    
    private void logBeforeRoute(final String type, final String logicTable, final Collection<?> targets, final Collection<String> shardingColumns, final List<ShardingValue<?>> shardingValues) {
        log.debug("Before {} sharding {} routes db names: {} sharding columns: {} sharding values: {}", type, logicTable, targets, shardingColumns, shardingValues);
    }
    
    private void logAfterRoute(final String type, final String logicTable, final Collection<String> shardingResults) {
        log.debug("After {} sharding {} result: {}", type, logicTable, shardingResults);
    }
    
    private RoutingResult generateRoutingResult(final TableRule tableRule, final Collection<String> routedDataSources, final Collection<String> routedTables) {
        RoutingResult result = new RoutingResult();
        for (DataNode each : tableRule.getActualDataNodes(routedDataSources, routedTables)) {
            result.getTableUnits().getTableUnits().add(new TableUnit(each.getDataSourceName(), logicTableName, each.getTableName()));
        }
        return result;
    }
}
