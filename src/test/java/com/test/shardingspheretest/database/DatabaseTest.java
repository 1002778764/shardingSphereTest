package com.test.shardingspheretest.database;


import com.zaxxer.hikari.util.DriverDataSource;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.keygen.KeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;


@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
public class DatabaseTest {
    @Container
    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("test")
            .withUsername("test")
            .withPassword("test");

    private DataSource shardingDataSource;

    @BeforeAll
    public void setUpBeforeClass() throws SQLException {
        try (Connection connection = DriverManager.getConnection(postgresContainer.getJdbcUrl(),postgresContainer.getUsername(),postgresContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                // 删除 users 表
                statement.executeUpdate("DROP TABLE IF EXISTS users");
                // 创建 users 表
                statement.executeUpdate("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255), age INT)");
                statement.executeUpdate("INSERT INTO users (name, age) VALUES ('John', 25)");
                statement.executeUpdate("INSERT INTO users (name, age) VALUES ('Jane', 30)");
            }
        }
        // 创建Sharding数据源
        createShardingDataSource();
    }

    private void createShardingDataSource() throws SQLException {
        ShardingRuleConfiguration rule = new ShardingRuleConfiguration();
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration("","");
        tableRule.setTableShardingStrategy(new StandardShardingStrategyConfiguration("",""));
        tableRule.setKeyGenerateStrategy(new KeyGenerateStrategyConfiguration("",""));
        rule.getTables().add(tableRule);
        rule.getShardingAlgorithms().put("", new ShardingSphereAlgorithmConfiguration("", new Properties()));
        rule.getKeyGenerators().put("", new ShardingSphereAlgorithmConfiguration("", new Properties()));
        Set<RuleConfiguration> ruleConfigs = new HashSet<>();
        ruleConfigs.add(rule);
        shardingDataSource = ShardingSphereDataSourceFactory.createDataSource(createDataSourceMap(), ruleConfigs, getProperties());
    }

    @Test
    public void testDatabase() throws SQLException {
        // 获取数据库连接
        try (Connection connection = shardingDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            // 插入数据
            statement.executeUpdate("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 20)");
            statement.executeUpdate("INSERT INTO users (id, name, age) VALUES (1001, 'Bob', 25)");

            // 查询数据
            ResultSet resultSet = statement.executeQuery("SELECT * FROM users");
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                System.out.println("ID: " + id + ", Name: " + name + ", Age: " + age);
            }
            resultSet.close();
        }
    }

    private Map<String, DataSource> createDataSourceMap() {
        DataSource dataSource = new DriverDataSource(postgresContainer.getJdbcUrl(),"",new Properties(),postgresContainer.getUsername(),postgresContainer.getPassword());
        Map<String, DataSource> result = new HashMap<>();
        result.put("ds0", dataSource);
        return result;
    }

    private Properties getProperties() {
        Properties shardingProperties = new Properties();
        shardingProperties.put("sql.show", true);
        return shardingProperties;
    }
}
