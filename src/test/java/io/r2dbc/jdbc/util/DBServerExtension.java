/*
 * Copyright 2017-2018 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed
 * to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

package io.r2dbc.jdbc.util;

import javax.sql.DataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Thomas Freese
 */
public final class DBServerExtension implements BeforeAllCallback, AfterAllCallback
{
    /**
     *
     */
    private HikariDataSource dataSource;

    /**
     *
     */
    private JdbcOperations jdbcOperations;

    /**
     * Erstellt ein neues {@link DBServerExtension} Object.
     */
    public DBServerExtension()
    {
        super();
    }

    /**
     * @see org.junit.jupiter.api.extension.AfterAllCallback#afterAll(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void afterAll(final ExtensionContext context)
    {
        this.dataSource.close();
    }

    /**
     * @see org.junit.jupiter.api.extension.BeforeAllCallback#beforeAll(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void beforeAll(final ExtensionContext context) throws Exception
    {
        // Class.forName(getDriver(), true, getClass().getClassLoader());

        // @formatter:off
//        this.dataSource = DataSourceBuilder.create().type(HikariDataSource.class)
//                .driverClassName("org.hsqldb.jdbc.JDBCDriver")
//                .url("jdbc:hsqldb:mem:%d"+ System.nanoTime())
//                .username("sa")
//                .password("")
//                .build()
//                ;
        // @formatter:on

        // ;MVCC=true;LOCK_MODE=0

        this.dataSource = new HikariDataSource();
        this.dataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
        this.dataSource.setJdbcUrl("jdbc:hsqldb:mem:" + System.nanoTime());
        // this.dataSource.setDriverClassName("org.h2.Driver");
        // this.dataSource.setJdbcUrl("jdbc:h2:mem:%d" + System.nanoTime());
        this.dataSource.setUsername("sa");
        this.dataSource.setPassword("");

        this.dataSource.setMaximumPoolSize(2);
        // this.dataSource.setConnectionTimeout(TimeUnit.MINUTES.toMillis(5));

        this.jdbcOperations = new JdbcTemplate(this.dataSource);
    }

    /**
     * @return {@link DataSource}
     */
    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    /**
     * @return String
     */
    public String getDriver()
    {
        return this.dataSource.getDriverClassName();
    }

    /**
     * @return {@link JdbcOperations}
     */
    public JdbcOperations getJdbcOperations()
    {
        return this.jdbcOperations;
    }

    /**
     * @return String
     */
    public String getPassword()
    {
        return this.dataSource.getPassword();
    }

    /**
     * Test-Übergreifender Object-Store.
     *
     * @param context {@link ExtensionContext}
     * @return {@link Store}
     */
    Store getStoreForClass(final ExtensionContext context)
    {
        return context.getStore(Namespace.create(getClass()));
    }

    /**
     * Test-Übergreifender Object-Store.
     *
     * @param context {@link ExtensionContext}
     * @return {@link Store}
     */
    Store getStoreForGlobal(final ExtensionContext context)
    {
        return context.getStore(Namespace.create("global"));
    }

    /**
     * Test-Übergreifender Object-Store.
     *
     * @param context {@link ExtensionContext}
     * @return {@link Store}
     */
    Store getStoreForMethod(final ExtensionContext context)
    {
        return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
    }

    /**
     * @return String
     */
    public String getUrl()
    {
        return this.dataSource.getJdbcUrl();
    }

    /**
     * @return String
     */
    public String getUsername()
    {
        return this.dataSource.getUsername();
    }
}
