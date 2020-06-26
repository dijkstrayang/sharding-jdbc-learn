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

package com.dangdang.ddframe.rdb.sharding.routing.strategy;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.constant.SQLType;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;

/**
 * 分片策略.
 * 
 * @author zhangliang
 */
public class ShardingStrategy {
    
    @Getter
    private final Collection<String> shardingColumns;
    
    private final ShardingAlgorithm shardingAlgorithm;
    
    public ShardingStrategy(final String shardingColumn, final ShardingAlgorithm shardingAlgorithm) {
        this(Collections.singletonList(shardingColumn), shardingAlgorithm);
    }
    
    public ShardingStrategy(final Collection<String> shardingColumns, final ShardingAlgorithm shardingAlgorithm) {
        this.shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        this.shardingColumns.addAll(shardingColumns);
        this.shardingAlgorithm = shardingAlgorithm;
    }
    
    /**
     * 计算静态分片.
     *
     * @param sqlType SQL语句的类型
     * @param availableTargetNames 所有的可用分片资源集合
     * @param shardingValues 分片值集合
     * @return 分库后指向的数据源名称集合
     */
    public Collection<String> doStaticSharding(final SQLType sqlType, final Collection<String> availableTargetNames, final Collection<ShardingValue<?>> shardingValues) {
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // 判断分片值是否为空
        if (shardingValues.isEmpty()) {
            // 分片值为空，则判断SQLType是否为insert类型 ,如果是insert类型，则报异常

            // 在后续版本里面，作者把这个校验代码摘除了
            // 这也导致了，如果做了分表，但是insert语句里面不包含分表的分片字段，**那么将会把路由的到的表里面全部插入该数据
            Preconditions.checkState(!isInsertMultiple(sqlType, availableTargetNames), "INSERT statement should contain sharding value.");
            // 返回所有目标资源
            result.addAll(availableTargetNames);
        } else {
            // 调用分片方法
            result.addAll(doSharding(shardingValues, availableTargetNames));
        }
        return result;
    }
    
    /**
     * 计算动态分片.
     *
     * @param shardingValues 分片值集合
     * @return 分库后指向的分片资源集合
     */
    public Collection<String> doDynamicSharding(final Collection<ShardingValue<?>> shardingValues) {
        // 动态分片，分片值必须要有，否则报错
        Preconditions.checkState(!shardingValues.isEmpty(), "Dynamic table should contain sharding value.");
        Collection<String> availableTargetNames = Collections.emptyList();
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // 调用分片算法
        result.addAll(doSharding(shardingValues, availableTargetNames));
        return result;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Collection<String> doSharding(final Collection<ShardingValue<?>> shardingValues, final Collection<String> availableTargetNames) {
        // sharding算法分为三种：NoneKey，SingleKey和MultipleKeys
        if (shardingAlgorithm instanceof NoneKeyShardingAlgorithm) {
            return Collections.singletonList(((NoneKeyShardingAlgorithm) shardingAlgorithm).doSharding(availableTargetNames, shardingValues.iterator().next()));
        }
        if (shardingAlgorithm instanceof SingleKeyShardingAlgorithm) {
            SingleKeyShardingAlgorithm<?> singleKeyShardingAlgorithm = (SingleKeyShardingAlgorithm<?>) shardingAlgorithm;
            ShardingValue shardingValue = shardingValues.iterator().next();
            switch (shardingValue.getType()) {
                case SINGLE:
                    return Collections.singletonList(singleKeyShardingAlgorithm.doEqualSharding(availableTargetNames, shardingValue));
                case LIST:
                    return singleKeyShardingAlgorithm.doInSharding(availableTargetNames, shardingValue);
                case RANGE:
                    return singleKeyShardingAlgorithm.doBetweenSharding(availableTargetNames, shardingValue);
                default:
                    throw new UnsupportedOperationException(shardingValue.getType().getClass().getName());
            }
        }
        if (shardingAlgorithm instanceof MultipleKeysShardingAlgorithm) {
            return ((MultipleKeysShardingAlgorithm) shardingAlgorithm).doSharding(availableTargetNames, shardingValues);
        }
        throw new UnsupportedOperationException(shardingAlgorithm.getClass().getName());
    }
    
    private boolean isInsertMultiple(final SQLType sqlType, final Collection<String> availableTargetNames) {
        return SQLType.INSERT == sqlType && availableTargetNames.size() > 1;
    }
}
