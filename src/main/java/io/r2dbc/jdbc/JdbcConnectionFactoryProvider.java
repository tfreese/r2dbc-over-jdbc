// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public final class JdbcConnectionFactoryProvider implements ConnectionFactoryProvider {
    public static final Option<Codecs> CODECS = Option.valueOf("codecs");

    public static final Option<DataSource> DATASOURCE = Option.valueOf("datasource");

    @Override
    public ConnectionFactory create(final ConnectionFactoryOptions connectionFactoryOptions) {
        Objects.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        // @formatter:off
        JdbcConnectionConfiguration.Builder builder = JdbcConnectionConfiguration.builder()
                .dataSource((DataSource) connectionFactoryOptions.getValue(DATASOURCE))
                .codecs((Codecs) connectionFactoryOptions.getValue(CODECS));
        // @formatter:on

        return new JdbcConnectionFactory(builder.build());
    }

    @Override
    public String getDriver() {
        return "generic";
    }

    @Override
    public boolean supports(final ConnectionFactoryOptions connectionFactoryOptions) {
        Objects.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        Object dataSource = connectionFactoryOptions.getValue(DATASOURCE);

        return dataSource != null;
    }
}
