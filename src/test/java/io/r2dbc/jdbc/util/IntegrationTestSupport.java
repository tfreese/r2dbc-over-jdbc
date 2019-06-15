/*
 * Copyright 2018-2019 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed
 * to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

package io.r2dbc.jdbc.util;

import io.r2dbc.jdbc.JdbcConnection;
import io.r2dbc.jdbc.JdbcConnectionFactory;
import io.r2dbc.jdbc.JdbcConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;


/**
 * Support class for integration tests.
 *
 * @author Thomas Freese
 */
public abstract class IntegrationTestSupport
{
    /**
     *
     */
    @RegisterExtension
    protected static final HsqldbServerExtension SERVER = new HsqldbServerExtension();

    /**
     *
     */
    protected static JdbcConnection connection;

    /**
     *
     */
    protected static JdbcConnectionFactory connectionFactory;

    /**
     *
     */
    @AfterAll
    static void afterAll()
    {
        if (connection != null)
        {
            connection.close().subscribe();
        }
    }

    /**
     *
     */
    @BeforeAll
    static void beforeAll()
    {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, SERVER.getDataSource()).build();

        connectionFactory = (JdbcConnectionFactory) ConnectionFactories.get(options);
        connection = connectionFactory.create().block();
    }
}