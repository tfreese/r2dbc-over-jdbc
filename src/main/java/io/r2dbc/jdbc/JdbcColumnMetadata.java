// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.util.Objects;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcColumnMetadata implements ColumnMetadata
{
    /**
     *
     */
    private final JDBCType jdbcType;

    /**
     *
     */
    private final String name;

    /**
     *
     */
    private final Nullability nullability;

    /**
     *
     */
    private final int precision;

    /**
     *
     */
    private final int scale;

    /**
     * Erstellt ein neues {@link JdbcColumnMetadata} Object.
     *
     * @param name String
     * @param jdbcType {@link JDBCType}
     * @param nullability {@link Nullability}
     * @param precision int
     * @param scale int
     */
    public JdbcColumnMetadata(final String name, final JDBCType jdbcType, final Nullability nullability, final int precision, final int scale)
    {
        super();

        this.name = Objects.requireNonNull(name, "name required");
        this.jdbcType = Objects.requireNonNull(jdbcType, "jdbcType required");
        this.nullability = Objects.requireNonNull(nullability, "nullability required");
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof JdbcColumnMetadata))
        {
            return false;
        }

        JdbcColumnMetadata other = (JdbcColumnMetadata) obj;

        return (this.jdbcType == other.jdbcType) && Objects.equals(this.name, other.name) && (this.nullability == other.nullability)
                && (this.precision == other.precision) && (this.scale == other.scale);
    }

    /**
     * @see io.r2dbc.spi.ColumnMetadata#getName()
     */
    @Override
    public String getName()
    {
        return this.name;
    }

    /**
     * @see io.r2dbc.spi.ColumnMetadata#getNativeTypeMetadata()
     */
    @Override
    public Object getNativeTypeMetadata()
    {
        return this.jdbcType;
    }

    /**
     * @see io.r2dbc.spi.ColumnMetadata#getNullability()
     */
    @Override
    public Nullability getNullability()
    {
        return this.nullability;
    }

    /**
     * @see io.r2dbc.spi.ColumnMetadata#getPrecision()
     */
    @Override
    public Integer getPrecision()
    {
        return this.precision;
    }

    /**
     * @see io.r2dbc.spi.ColumnMetadata#getScale()
     */
    @Override
    public Integer getScale()
    {
        return this.scale;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(this.jdbcType, this.name, this.nullability, this.precision, this.scale);
    }
}
