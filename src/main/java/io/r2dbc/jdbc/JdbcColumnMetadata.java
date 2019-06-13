/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
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
    private static final Map<Integer, Class<?>> SQL_TO_JAVATYPE_MAP;

    /**
     * {@link JDBCType}, {@link Types}
     */
    static
    {
        SQL_TO_JAVATYPE_MAP = new HashMap<>();
        SQL_TO_JAVATYPE_MAP.put(JDBCType.BIGINT.getVendorTypeNumber(), Long.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.BINARY.getVendorTypeNumber(), byte[].class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.BIT.getVendorTypeNumber(), Boolean.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.BLOB.getVendorTypeNumber(), byte[].class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.BOOLEAN.getVendorTypeNumber(), Boolean.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.CHAR.getVendorTypeNumber(), Character.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.CLOB.getVendorTypeNumber(), String.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.DATE.getVendorTypeNumber(), LocalDate.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.DECIMAL.getVendorTypeNumber(), Long.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.DOUBLE.getVendorTypeNumber(), Double.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.FLOAT.getVendorTypeNumber(), Float.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.INTEGER.getVendorTypeNumber(), Integer.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.NUMERIC.getVendorTypeNumber(), Double.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.SMALLINT.getVendorTypeNumber(), Short.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.TIME.getVendorTypeNumber(), LocalTime.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.TIMESTAMP.getVendorTypeNumber(), LocalDateTime.class);
        SQL_TO_JAVATYPE_MAP.put(JDBCType.VARCHAR.getVendorTypeNumber(), String.class);
    }

    /**
     *
     */
    private final Class<?> javaType;

    /**
     *
     */
    private final String name;

    /**
     *
     */
    private final int nativeType;

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
     * @param javaType Class
     * @param name String
     * @param nativeType int
     * @param nullability {@link Nullability}
     * @param precision int
     * @param scale int
     */
    public JdbcColumnMetadata(final Class<?> javaType, final String name, final int nativeType, final Nullability nullability, final int precision,
            final int scale)
    {
        super();

        this.javaType = javaType != null ? javaType : SQL_TO_JAVATYPE_MAP.get(nativeType);
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.nativeType = nativeType;
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

        // if (this.javaType == null)
        // {
        // if (other.javaType != null)
        // {
        // return false;
        // }
        // }
        // else if (!this.javaType.equals(other.javaType))
        // {
        // return false;
        // }

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

        if (this.nativeType != other.nativeType)
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
     * @see io.r2dbc.spi.ColumnMetadata#getJavaType()
     */
    @Override
    public Class<?> getJavaType()
    {
        return this.javaType;
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
        return this.nativeType;
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

        // result = prime * result + ((this.javaType == null) ? 0 : this.javaType.hashCode());
        result = (prime * result) + ((this.name == null) ? 0 : this.name.hashCode());
        result = (prime * result) + this.nativeType;
        result = (prime * result) + ((this.nullability == null) ? 0 : this.nullability.hashCode());
        result = (prime * result) + this.precision;
        result = (prime * result) + this.scale;

        return result;
    }
}
