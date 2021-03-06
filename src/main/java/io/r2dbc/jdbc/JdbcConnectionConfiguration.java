// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.jdbc.codecs.DefaultCodecs;

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
        private Codecs codecs;

        /**
        *
        */
        private DataSource dataSource;

        /**
         * @return {@link JdbcConnectionConfiguration}
         */
        public JdbcConnectionConfiguration build()
        {
            if (this.codecs == null)
            {
                this.codecs = new DefaultCodecs();
            }

            return new JdbcConnectionConfiguration(this.dataSource, this.codecs);

        }

        /**
         * @param codecs {@link Codecs}
         * @return {@link Builder}
         */
        public Builder codecs(final Codecs codecs)
        {
            this.codecs = codecs;

            return this;
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

        /**
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append("Builder [");
            builder.append("dataSource=").append(this.dataSource);
            builder.append(", codecs=").append(this.codecs);
            builder.append("]");

            return builder.toString();
        }
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
    private final Codecs codecs;

    /**
    *
    */
    private final DataSource dataSource;

    /**
     * Erstellt ein neues {@link JdbcConnectionConfiguration} Object.
     *
     * @param dataSource {@link DataSource}
     * @param codecs {@link Codecs}
     */
    private JdbcConnectionConfiguration(final DataSource dataSource, final Codecs codecs)
    {
        super();

        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.codecs = Objects.requireNonNull(codecs, "codecs must not be null");
    }

    /**
     * @return {@link Codecs}
     */
    public Codecs getCodecs()
    {
        return this.codecs;
    }

    /**
     * @return {@link DataSource}
     */
    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("JdbcConnectionConfiguration [");
        builder.append("dataSource=").append(this.dataSource);
        builder.append(", codecs=").append(this.codecs);
        builder.append("]");

        return builder.toString();
    }
}
