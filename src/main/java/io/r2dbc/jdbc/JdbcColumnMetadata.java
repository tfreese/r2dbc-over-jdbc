/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

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
     *
     */
    private final int sqlType;

    /**
     * Erstellt ein neues {@link JdbcColumnMetadata} Object.
     *
     * @param name String
     * @param sqlType int
     * @param nullability {@link Nullability}
     * @param precision int
     * @param scale int
     */
    public JdbcColumnMetadata(final String name, final int sqlType, final Nullability nullability, final int precision, final int scale)
    {
        super();

        this.name = Objects.requireNonNull(name, "name must not be null");
        this.sqlType = sqlType;
        this.nullability = Objects.requireNonNull(nullability, "nullability must not be null");
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

        if (obj == null)
        {
            return false;
        }

        if (getClass() != obj.getClass())
        {
            return false;
        }

        JdbcColumnMetadata other = (JdbcColumnMetadata) obj;

        if (this.name == null)
        {
            if (other.name != null)
            {
                return false;
            }
        }
        else if (!this.name.equals(other.name))
        {
            return false;
        }

        if (this.sqlType != other.sqlType)
        {
            return false;
        }

        if (this.nullability != other.nullability)
        {
            return false;
        }

        if (this.precision != other.precision)
        {
            return false;
        }

        if (this.scale != other.scale)
        {
            return false;
        }

        return true;
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
        return this.sqlType;
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
        final int prime = 31;
        int result = 1;

        // result = (prime * result) + ((this.javaType == null) ? 0 : this.javaType.hashCode());
        result = (prime * result) + ((this.name == null) ? 0 : this.name.hashCode());
        result = (prime * result) + this.sqlType;
        result = (prime * result) + ((this.nullability == null) ? 0 : this.nullability.hashCode());
        result = (prime * result) + this.precision;
        result = (prime * result) + this.scale;

        return result;
    }
}
