package com.test.shardingspheretest.config;

import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.keygen.KeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * sharding 配置信息
 *
 * @author ruoyi
 */
@Configuration
public class ShardingDataSourceConfig {

    @Resource
    private DataSource henhouseDataSource;
    @Resource
    private DataSource beehiveDataSource;

    @Bean(name = "shardingDataSource")
    public DataSource shardingDataSource() throws SQLException {
        // 获取数据源对象
        return ShardingSphereDataSourceFactory.createDataSource(createDataSourceMap(), Collections.singleton(createShardingRuleConfiguration()), getProperties());
    }

    /**
     * 分片规则配置
     *
     * @return
     */
    private ShardingRuleConfiguration createShardingRuleConfiguration() {
        ShardingRuleConfiguration rule = new ShardingRuleConfiguration();
        rule.getTables().add(getHenhouseTableRuleConfiguration());
        rule.getTables().add(getBeehiveTableRuleConfiguration());
        // 鸡舍数据表分片算法
        Properties propsHenhouse = new Properties();
        propsHenhouse.setProperty("strategy","STANDARD");
        propsHenhouse.setProperty("algorithmClassName","com.bee.framework.config.sharding.TimeHenhouseStandardShardingAlgorithmFixture");
        rule.getShardingAlgorithms().put("sharding-algorithm-henhouse", new ShardingSphereAlgorithmConfiguration("CLASS_BASED",propsHenhouse));
        // 蜂箱数据表分片算法
        Properties propsBeehive = new Properties();
        propsBeehive.setProperty("strategy","STANDARD");
        propsBeehive.setProperty("algorithmClassName","com.bee.framework.config.sharding.TimeBeehiveStandardShardingAlgorithmFixture");
        rule.getShardingAlgorithms().put("sharding-algorithm-beehive", new ShardingSphereAlgorithmConfiguration("CLASS_BASED",propsBeehive));
        // 主键生成策略：雪花算法
        Properties snowflakeProp = new Properties();
        snowflakeProp.setProperty("worker-id", "1");
        snowflakeProp.setProperty("datacenter-id", "1");
        snowflakeProp.setProperty("strategy","SNOWFLAKE");
        rule.getKeyGenerators().put("alg-snowflake", new ShardingSphereAlgorithmConfiguration("SNOWFLAKE", snowflakeProp));
        return rule;
    }

    /**
     * 鸡舍分片表配置
     *
     * @return
     */
    private ShardingTableRuleConfiguration getHenhouseTableRuleConfiguration() {
        /**
         * 配置数据节点
         * 逻辑表：hen_house_smart_data
         * 数据节点（数据源 + 真实表名）：
         * data_source_henhouse_0.hen_house_smart_data_$->{2023..2100}0$->{1..9},data_source_henhouse_0.hen_house_smart_data_$->{2023..2100}1$->{0..2}
         */
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration("hen_house_smart_data", "data_source_henhouse_0.hen_house_smart_data_$->{2023..2100}0$->{1..9},data_source_henhouse_0.hen_house_smart_data_$->{2023..2100}1$->{0..2}");
        /**
         * 配置分表策略：水平分片策略。
         * 分片列名称：create_time_data
         * 分片算法名称：sharding-algorithm-henhouse
         */
        StandardShardingStrategyConfiguration standard = new StandardShardingStrategyConfiguration("create_time_data", "sharding-algorithm-henhouse");
        tableRule.setTableShardingStrategy(standard);
        /**
         * 配置主键生成策略
         * 主键：smart_data_id
         * 雪花算法名称：snowflake
         */
        KeyGenerateStrategyConfiguration keyGenerate = new KeyGenerateStrategyConfiguration("smart_data_id", "alg-snowflake");
        tableRule.setKeyGenerateStrategy(keyGenerate);
        return tableRule;
    }

    /**
     * 蜂箱分片表配置
     *
     * @return
     */
    private ShardingTableRuleConfiguration getBeehiveTableRuleConfiguration() {
        /**
         * 配置数据节点
         * 逻辑表：bee_beehive_data
         * 数据节点（数据源 + 真实表名）：
         * data_source_beehive_0.bee_beehive_data_$->{2022..2100}0$->{1..9},data_source_beehive_0.bee_beehive_data_$->{2022..2100}1$->{0..2}
         */
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration("bee_beehive_data", "data_source_beehive_0.bee_beehive_data_$->{2022..2100}0$->{1..9},data_source_beehive_0.bee_beehive_data_$->{2022..2100}1$->{0..2}");
        /**
         * 配置分表策略：水平分片策略。
         * 分片列名称：create_date_time
         * 分片算法名称：sharding-algorithm-beehive
         */
        StandardShardingStrategyConfiguration standard = new StandardShardingStrategyConfiguration("create_date_time", "sharding-algorithm-beehive");
        tableRule.setTableShardingStrategy(standard);
        /**
         * 配置主键生成策略
         * 主键：beehive_data_id
         * 雪花算法名称：snowflake
         */
        KeyGenerateStrategyConfiguration keyGenerate = new KeyGenerateStrategyConfiguration("beehive_data_id", "alg-snowflake");
        tableRule.setKeyGenerateStrategy(keyGenerate);
        return tableRule;
    }

    /**
     * 数据源配置
     *
     * @return
     */
    private Map<String, DataSource> createDataSourceMap() {
        Map<String, DataSource> result = new HashMap<>();
        result.put("data_source_henhouse_0", henhouseDataSource);
        result.put("data_source_beehive_0", beehiveDataSource);
        return result;
    }

    /**
     * 系统参数配置
     */
    private Properties getProperties() {
        Properties shardingProperties = new Properties();
        shardingProperties.put("sql.show", true);
        return shardingProperties;
    }
}