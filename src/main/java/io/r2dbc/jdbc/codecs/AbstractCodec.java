// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Basic-Implementation for a {@link Codec}.
 *
 * @author Thomas Freese
 */
public abstract class AbstractCodec<T> implements Codec<T> {
    private final Class<T> javaType;

    private final Set<JDBCType> supportedJdbcTypes;

    // @SuppressWarnings("unchecked")
    // protected AbstractCodec()
    // {
    // super();
    //
    // this.javaType = (Class<T>) ((ParameterizedType) (getClass().getGenericSuperclass())).getActualTypeArguments()[0];
    // }

    protected AbstractCodec(final Class<T> javaType, final JDBCType... supportedJdbcTypes) {
        super();

        this.javaType = Objects.requireNonNull(javaType, "javaType required");

        if (supportedJdbcTypes.length == 0) {
            this.supportedJdbcTypes = Collections.emptySet();
        }
        else {
            this.supportedJdbcTypes = Stream.of(supportedJdbcTypes).collect(Collectors.toUnmodifiableSet());
        }
    }

    @Override
    public Class<T> getJavaType() {
        return this.javaType;
    }

    @Override
    public Set<JDBCType> supportedJdbcTypes() {
        return this.supportedJdbcTypes;
    }

    protected RuntimeException throwCanNotMapException(final Object object) {
        return new IllegalArgumentException(String.format("can not map %s into %s", object.getClass().getSimpleName(), getJavaType().getSimpleName()));
    }
}
