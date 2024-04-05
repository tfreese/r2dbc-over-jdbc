// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.util.Objects;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Type;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcColumnMetadata implements ColumnMetadata, Type {
    private final int column;
    private final Class<?> javaType;
    private final JDBCType jdbcType;
    private final String name;
    private final Nullability nullability;
    private final int precision;
    private final int scale;

    public JdbcColumnMetadata(final String name, final int column, final Class<?> javaType, final JDBCType jdbcType, final Nullability nullability, final int precision,
                              final int scale) {
        super();

        this.name = Objects.requireNonNull(name, "name required").toLowerCase();
        this.column = column;
        this.jdbcType = Objects.requireNonNull(jdbcType, "jdbcType required");
        this.javaType = Objects.requireNonNull(javaType, "javaType required");
        this.nullability = Objects.requireNonNull(nullability, "nullability required");
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof JdbcColumnMetadata other)) {
            return false;
        }

        return this.column == other.column && Objects.equals(this.javaType, other.javaType) && this.jdbcType == other.jdbcType && Objects.equals(this.name,
                other.name) && this.nullability == other.nullability && this.precision == other.precision && this.scale == other.scale;
    }

    public int getColumn() {
        return this.column;
    }

    @Override
    public Class<?> getJavaType() {
        return this.javaType;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public JDBCType getNativeTypeMetadata() {
        return this.jdbcType;
    }

    @Override
    public Nullability getNullability() {
        return this.nullability;
    }

    @Override
    public Integer getPrecision() {
        return this.precision;
    }

    @Override
    public Integer getScale() {
        return this.scale;
    }

    @Override
    public Type getType() {
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.column, this.javaType, this.jdbcType, this.name, this.nullability, this.precision, this.scale);
    }
}
