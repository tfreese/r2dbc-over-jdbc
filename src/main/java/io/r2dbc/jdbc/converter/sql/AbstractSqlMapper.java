// Created: 14.06.2019
package io.r2dbc.jdbc.converter.sql;

import java.lang.reflect.ParameterizedType;
import java.util.Objects;

/**
 * Basic-Implementation for a {@link SqlMapper}.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public abstract class AbstractSqlMapper<T> implements SqlMapper<T>
{
    /**
    *
    */
    private final Class<T> javaType;

    /**
     * Erstellt ein neues {@link AbstractSqlMapper} Object.
     */
    @SuppressWarnings("unchecked")
    protected AbstractSqlMapper()
    {
        super();

        this.javaType = (Class<T>) ((ParameterizedType) (getClass().getGenericSuperclass())).getActualTypeArguments()[0];
    }

    /**
     * Erstellt ein neues {@link AbstractSqlMapper} Object.
     *
     * @param javaType Class
     */
    protected AbstractSqlMapper(final Class<T> javaType)
    {
        super();

        this.javaType = Objects.requireNonNull(javaType, "javaType required");
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#getJavaType()
     */
    @Override
    public Class<T> getJavaType()
    {
        return this.javaType;
    }
}
