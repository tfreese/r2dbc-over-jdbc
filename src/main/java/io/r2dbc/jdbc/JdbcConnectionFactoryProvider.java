// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public final class JdbcConnectionFactoryProvider implements ConnectionFactoryProvider
{
    // /**
    // * Options, Semicolon delimited
    // */
    // public static final Option<String> OPTIONS = Option.valueOf("options");
    //
    // /**
    // * Url
    // */
    // public static final Option<String> URL = Option.valueOf("url");

    /**
     * {@link DataSource}
     */
    public static final Option<DataSource> DATASOURCE = Option.valueOf("datasource");

    /**
     * @see io.r2dbc.spi.ConnectionFactoryProvider#create(io.r2dbc.spi.ConnectionFactoryOptions)
     */
    @Override
    public ConnectionFactory create(final ConnectionFactoryOptions connectionFactoryOptions)
    {
        Objects.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        DataSource dataSource = connectionFactoryOptions.getValue(DATASOURCE);

        JdbcConnectionConfiguration.Builder builder = JdbcConnectionConfiguration.builder();
        builder.dataSource(dataSource);

        // String userName = connectionFactoryOptions.getValue(USER);
        //
        // if (userName != null)
        // {
        // builder.username(userName);
        // }
        //
        // CharSequence password = connectionFactoryOptions.getValue(PASSWORD);
        //
        // if (password != null)
        // {
        // builder.password(password.toString());
        // }
        //
        // String url = connectionFactoryOptions.getValue(URL);
        //
        // if (url != null)
        // {
        // builder.url(url);
        // }
        //
        // String options = connectionFactoryOptions.getValue(OPTIONS);
        //
        // if (options != null)
        // {
        // for (String option : options.split(";"))
        // {
        // builder.option(option);
        // }
        // }

        return new JdbcConnectionFactory(builder.build());
    }

    /**
     * @see io.r2dbc.spi.ConnectionFactoryProvider#getDriver()
     */
    @Override
    public String getDriver()
    {
        return "generic";
    }

    /**
     * @see io.r2dbc.spi.ConnectionFactoryProvider#supports(io.r2dbc.spi.ConnectionFactoryOptions)
     */
    @Override
    public boolean supports(final ConnectionFactoryOptions connectionFactoryOptions)
    {
        Objects.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        DataSource dataSource = connectionFactoryOptions.getValue(DATASOURCE);

        // if (connectionFactoryOptions.hasOption(DATASOURCE))
        if (dataSource != null)
        {
            return true;
        }

        // String driver = connectionFactoryOptions.getValue(DRIVER);
        //
        // if ((driver == null) || driver.isBlank())
        // {
        // return false;
        // }
        //
        // if (connectionFactoryOptions.hasOption(URL))
        // {
        // return true;
        // }
        //
        // if (connectionFactoryOptions.hasOption(PROTOCOL) && connectionFactoryOptions.hasOption(DATABASE))
        // {
        // return true;
        // }

        return false;
    }
}
