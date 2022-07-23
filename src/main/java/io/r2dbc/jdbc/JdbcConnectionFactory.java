// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.jdbc.codecs.DefaultCodecs;
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
    private final Codecs codecs;
    /**
     *
     */
    private final Mono<Connection> jdbcConnectionFactory;

    /**
     * Erstellt ein neues {@link JdbcConnectionFactory} Object.
     *
     * @param dataSource {@link DataSource}
     * @param codecs {@link Codecs}
     */
    public JdbcConnectionFactory(final DataSource dataSource, final Codecs codecs)
    {
        super();

        Objects.requireNonNull(dataSource, "dataSource must not be null");

        this.jdbcConnectionFactory = Mono.fromCallable(dataSource::getConnection).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert);
        this.codecs = new DefaultCodecs();
    }

    /**
     * Erstellt ein neues {@link JdbcConnectionFactory} Object.
     *
     * @param connectionConfiguration {@link JdbcConnectionConfiguration}
     */
    public JdbcConnectionFactory(final JdbcConnectionConfiguration connectionConfiguration)
    {
        this(connectionConfiguration.getDataSource(), connectionConfiguration.getCodecs());
    }

    /**
     * @see io.r2dbc.spi.ConnectionFactory#create()
     */
    @Override
    public Mono<io.r2dbc.spi.Connection> create()
    {
        return this.jdbcConnectionFactory.map(jdbcConnection -> new JdbcConnection(jdbcConnection, this.codecs));
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
