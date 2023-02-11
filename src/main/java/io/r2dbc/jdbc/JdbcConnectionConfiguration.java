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
final class JdbcConnectionConfiguration {
    /**
     * @author Thomas Freese
     */
    static final class Builder {
        private Codecs codecs;

        private DataSource dataSource;

        public JdbcConnectionConfiguration build() {
            if (this.codecs == null) {
                this.codecs = new DefaultCodecs();
            }

            return new JdbcConnectionConfiguration(this.dataSource, this.codecs);

        }

        public Builder codecs(final Codecs codecs) {
            this.codecs = codecs;

            return this;
        }

        public Builder dataSource(final DataSource dataSource) {
            this.dataSource = dataSource;

            return this;
        }

        /**
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Builder [");
            builder.append("dataSource=").append(this.dataSource);
            builder.append(", codecs=").append(this.codecs);
            builder.append("]");

            return builder.toString();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final Codecs codecs;

    private final DataSource dataSource;

    private JdbcConnectionConfiguration(final DataSource dataSource, final Codecs codecs) {
        super();

        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.codecs = Objects.requireNonNull(codecs, "codecs must not be null");
    }

    public Codecs getCodecs() {
        return this.codecs;
    }

    public DataSource getDataSource() {
        return this.dataSource;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("JdbcConnectionConfiguration [");
        builder.append("dataSource=").append(this.dataSource);
        builder.append(", codecs=").append(this.codecs);
        builder.append("]");

        return builder.toString();
    }
}
