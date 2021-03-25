// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
final class JdbcConnectionConfiguration
{
    /**
     * @author Thomas Freese
     */
    static final class Builder
    {
        /**
        *
        */
        private DataSource dataSource;

        // /**
        // *
        // */
        // private String driver;
        //
        // /**
        // *
        // */
        // private final List<String> options = new ArrayList<>();
        //
        // /**
        // *
        // */
        // private String password;
        //
        // /**
        // *
        // */
        // private String url;
        //
        // /**
        // *
        // */
        // private String username;

        /**
         * Erstellt ein neues {@link Builder} Object.
         */
        public Builder()
        {
            super();
        }

        /**
         * @return {@link JdbcConnectionConfiguration}
         */
        public JdbcConnectionConfiguration build()
        {
            return new JdbcConnectionConfiguration(this.dataSource);

            // if (this.options.isEmpty())
            // {
            // return new JdbcConnectionConfiguration(this.driver, this.username, this.password, this.url);
            // }
            //
            // String urlWithOptions = this.options.stream().reduce(this.url, (url, option) -> url += ";" + option);
            //
            // return new JdbcConnectionConfiguration(this.driver, this.username, this.password, urlWithOptions);
        }

        /**
         * @param dataSource {@link DataSource}
         * @return {@link Builder}
         */
        public Builder dataSource(final DataSource dataSource)
        {
            this.dataSource = dataSource;

            return this;
        }

        // /**
        // * @param driver String
        // * @return {@link Builder}
        // */
        // public Builder driver(final String driver)
        // {
        // this.driver = driver;
        //
        // return this;
        // }
        //
        // /**
        // * @param option String
        // * @return {@link Builder}
        // */
        // public Builder option(final String option)
        // {
        // this.options.add(option);
        //
        // return this;
        // }
        //
        // /**
        // * @param password String
        // * @return {@link Builder}
        // */
        // public Builder password(final String password)
        // {
        // this.password = password;
        //
        // return this;
        // }

        /**
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append("Builder [");
            builder.append("dataSource=").append(this.dataSource);
            // builder.append(", username=").append(this.username);
            // builder.append(", password=").append(this.password);
            // builder.append(", url=").append(this.url);
            // builder.append(", options=").append(this.options);
            builder.append("]");

            return builder.toString();
        }

        // /**
        // * @param url String
        // * @return {@link Builder}
        // */
        // public Builder url(final String url)
        // {
        // this.url = Objects.requireNonNull(url, "url must not be null");
        //
        // return this;
        // }

        // /**
        // * @param username String
        // * @return {@link Builder}
        // */
        // public Builder username(final String username)
        // {
        // this.username = username;
        //
        // return this;
        // }
    }

    /**
     * @return {@link Builder}
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
    *
    */
    private final DataSource dataSource;

    // /**
    // *
    // */
    // private final String driver;
    //
    // /**
    // *
    // */
    // private final String password;
    //
    // /**
    // *
    // */
    // private final String url;
    //
    // /**
    // *
    // */
    // private final String username;

    /**
     * Erstellt ein neues {@link JdbcConnectionConfiguration} Object.
     *
     * @param dataSource {@link DataSource}
     */
    private JdbcConnectionConfiguration(final DataSource dataSource)
    {
        super();

        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    // /**
    // * Erstellt ein neues {@link JdbcConnectionConfiguration} Object.
    // *
    // * @param driver String
    // * @param username String
    // * @param password String
    // * @param url String
    // */
    // private JdbcConnectionConfiguration(final String driver, final String username, final String password, final String url)
    // {
    // super();
    //
    // this.driver = Objects.requireNonNull(driver, "driver must not be null");
    // this.username = username;
    // this.password = password;
    // this.url = Objects.requireNonNull(url, "url must not be null");
    // }
    //
    // /**
    // * @return String
    // */
    // String getDriver()
    // {
    // return this.driver;
    // }

    /**
     * @return {@link DataSource}
     */
    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    // /**
    // * @return {@link Optional}
    // */
    // Optional<String> getPassword()
    // {
    // return Optional.ofNullable(this.password);
    // }
    //
    // /**
    // * @return String
    // */
    // String getUrl()
    // {
    // return this.url;
    // }
    //
    // /**
    // * @return {@link Optional}
    // */
    // Optional<String> getUsername()
    // {
    // return Optional.ofNullable(this.username);
    // }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("JdbcConnectionConfiguration [");
        builder.append("dataSource=").append(this.dataSource);
        // builder.append(", driver=").append(this.driver);
        // builder.append(", username=").append(this.username);
        // builder.append(", password=").append(this.password);
        // builder.append(", url=").append(this.url);
        builder.append("]");

        return builder.toString();
    }
}
