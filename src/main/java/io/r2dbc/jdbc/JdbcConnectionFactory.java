/*
 * Copyright 2018 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package io.r2dbc.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public final class JdbcConnectionFactory implements ConnectionFactory
{
    /**
     *
     */
    private final Mono<Connection> connectionFactory;

    /**
     * Erstellt ein neues {@link JdbcConnectionFactory} Object.
     *
     * @param dataSource {@link DataSource}
     */
    public JdbcConnectionFactory(final DataSource dataSource)
    {
        super();

        Objects.requireNonNull(dataSource, "dataSource must not be null");

        this.connectionFactory = Mono.fromCallable(dataSource::getConnection).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create);
    }

    /**
     * Erstellt ein neues {@link JdbcConnectionFactory} Object.
     *
     * @param connectionConfiguration {@link JdbcConnectionConfiguration}
     */
    public JdbcConnectionFactory(final JdbcConnectionConfiguration connectionConfiguration)
    {
        this(connectionConfiguration.getDataSource());
    }

    /**
     * @see io.r2dbc.spi.ConnectionFactory#create()
     */
    @Override
    public Mono<JdbcConnection> create()
    {
        return this.connectionFactory.map(JdbcConnection::new);
    }

    /**
     * @see io.r2dbc.spi.ConnectionFactory#getMetadata()
     */
    @Override
    public ConnectionFactoryMetadata getMetadata()
    {
        return JdbcConnectionFactoryMetadata.INSTANCE;
    }
}
