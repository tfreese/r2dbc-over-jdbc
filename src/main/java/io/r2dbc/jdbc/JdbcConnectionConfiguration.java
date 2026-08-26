package io.r2dbc.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.jdbc.codecs.DefaultCodecs;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 * @since 14.06.2019
 */
public final class JdbcConnectionConfiguration {
    /**
     * @author Thomas Freese
     */
    public static final class Builder {
        private Codecs codecs;
        private DataSource dataSource;

        public JdbcConnectionConfiguration build() {
            if (codecs == null) {
                codecs = new DefaultCodecs();
            }

            return new JdbcConnectionConfiguration(dataSource, codecs);

        }

        public Builder codecs(final Codecs codecs) {
            this.codecs = codecs;

            return this;
        }

        public Builder dataSource(final DataSource dataSource) {
            this.dataSource = dataSource;

            return this;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append("Builder [");
            builder.append("dataSource=").append(dataSource);
            builder.append(", codecs=").append(codecs);
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

        this.dataSource = Objects.requireNonNull(dataSource, "dataSource required");
        this.codecs = Objects.requireNonNull(codecs, "codecs required");
    }

    public Codecs getCodecs() {
        return codecs;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("JdbcConnectionConfiguration [");
        builder.append("dataSource=").append(dataSource);
        builder.append(", codecs=").append(codecs);
        builder.append("]");

        return builder.toString();
    }
}
