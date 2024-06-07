// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public final class JdbcConnectionFactory implements ConnectionFactory {
    private final Codecs codecs;
    private final Mono<Connection> connectionFactory;

    public JdbcConnectionFactory(final DataSource dataSource, final Codecs codecs) {
        super();

        Objects.requireNonNull(dataSource, "dataSource must not be null");

        this.connectionFactory = Mono.fromCallable(dataSource::getConnection).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert);
        this.codecs = codecs;
    }

    public JdbcConnectionFactory(final JdbcConnectionConfiguration connectionConfiguration) {
        this(connectionConfiguration.getDataSource(), connectionConfiguration.getCodecs());
    }

    @Override
    public Mono<io.r2dbc.spi.Connection> create() {
        return this.connectionFactory.map(jdbcConnection -> new JdbcConnection(jdbcConnection, this.codecs));
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return JdbcConnectionFactoryMetadata.INSTANCE;
    }
}
