/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.converter;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.r2dbc.jdbc.converter.sql.BlobSqlMapper;
import io.r2dbc.jdbc.converter.sql.BooleanSqlMapper;
import io.r2dbc.jdbc.converter.sql.ClobSqlMapper;
import io.r2dbc.jdbc.converter.sql.DateSqlMapper;
import io.r2dbc.jdbc.converter.sql.DoubleSqlMapper;
import io.r2dbc.jdbc.converter.sql.FloatSqlMapper;
import io.r2dbc.jdbc.converter.sql.IntegerSqlMapper;
import io.r2dbc.jdbc.converter.sql.LongSqlMapper;
import io.r2dbc.jdbc.converter.sql.ObjectSqlMapper;
import io.r2dbc.jdbc.converter.sql.ShortSqlMapper;
import io.r2dbc.jdbc.converter.sql.SqlMapper;
import io.r2dbc.jdbc.converter.sql.StringSqlMapper;
import io.r2dbc.jdbc.converter.transformer.BlobTransformer;
import io.r2dbc.jdbc.converter.transformer.ByteTransformer;
import io.r2dbc.jdbc.converter.transformer.ClobTransformer;
import io.r2dbc.jdbc.converter.transformer.DateTransformer;
import io.r2dbc.jdbc.converter.transformer.DefaultObjectTransformer;
import io.r2dbc.jdbc.converter.transformer.DoubleTransformer;
import io.r2dbc.jdbc.converter.transformer.IntegerTransformer;
import io.r2dbc.jdbc.converter.transformer.LocalDateTimeConverter;
import io.r2dbc.jdbc.converter.transformer.LocalDateTransformer;
import io.r2dbc.jdbc.converter.transformer.LocalTimeTransformer;
import io.r2dbc.jdbc.converter.transformer.LongTransformer;
import io.r2dbc.jdbc.converter.transformer.ObjectTransformer;
import io.r2dbc.jdbc.converter.transformer.StringTransformer;

/**
 * Encodes and decodes objects.
 *
 * @author Thomas Freese
 */
public final class Converters
{
    /**
     * Fallback-Transformer.
     */
    public static final ObjectTransformer<?> FALLBACK_OBJECT_TRANSFORMER = DefaultObjectTransformer.INSTANCE;

    /**
     * Fallback-Mapper.
     */
    public static final SqlMapper<?> FALLBACK_SQL_MAPPER = ObjectSqlMapper.INSTANCE;

    /**
     *
     */
    private static final Converters INSTANCE = new Converters();

    /**
     *
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Converters.class);

    /**
     * @param javaType {@link Class}
     * @param <T> Type
     * @return {@link SqlMapper}
     */
    public static <T> SqlMapper<T> getSqlMapper(final Class<?> javaType)
    {
        return INSTANCE.findSqlMapper(javaType);
    }

    /**
     * @param jdbcType {@link JDBCType}
     * @param <T> Type
     * @return {@link SqlMapper}
     */
    public static <T> SqlMapper<T> getSqlMapper(final JDBCType jdbcType)
    {
        return INSTANCE.findSqlMapper(jdbcType);
    }

    /**
     * @param javaType {@link Class}
     * @param <T> Type
     * @return {@link ObjectTransformer}
     */
    public static <T> ObjectTransformer<T> getTransformer(final Class<?> javaType)
    {
        return INSTANCE.findTransformer(javaType);
    }

    /**
     * Register a new {@link SqlMapper} for a Java-Class.
     *
     * @param sqlMapper {@link SqlMapper}
     */
    public static void registerSqlMapper(final SqlMapper<?> sqlMapper)
    {
        INSTANCE.register(sqlMapper);
    }

    /**
     * @param transformer {@link ObjectTransformer}
     */
    public static void registerTransformer(final ObjectTransformer<?> transformer)
    {
        INSTANCE.register(transformer);
    }

    /**
    *
    */
    private final Map<Class<?>, SqlMapper<?>> javaTypeSqlMapperMap = new HashMap<>();

    /**
    *
    */
    private final Map<JDBCType, SqlMapper<?>> jdbcTypeSqlMapperMap = new HashMap<>();

    /**
    *
    */
    private final Map<Class<?>, ObjectTransformer<?>> transformerMap = new HashMap<>();

    /**
     * Erstellt ein neues {@link Converters} Object.
     */
    private Converters()
    {
        super();

        // Default SqlMapper
        register(new BlobSqlMapper());
        register(new BooleanSqlMapper());
        register(new ClobSqlMapper());
        register(new DateSqlMapper());
        register(new DoubleSqlMapper());
        register(new FloatSqlMapper());
        register(new IntegerSqlMapper());
        register(new LongSqlMapper());
        register(new ShortSqlMapper());
        register(new StringSqlMapper());

        // Default ObjectTransformer
        register(new ByteTransformer());
        register(new BlobTransformer());
        register(new ClobTransformer());
        register(new DateTransformer());
        register(new DoubleTransformer());
        register(new IntegerTransformer());
        register(new LocalDateTransformer());
        register(new LocalDateTimeConverter());
        register(new LocalTimeTransformer());
        register(new StringTransformer());
        register(new LongTransformer());
    }

    /**
     * @param <T> Type
     * @param javaType Class
     * @return {@link SqlMapper}
     */
    @SuppressWarnings("unchecked")
    private <T> SqlMapper<T> findSqlMapper(final Class<?> javaType)
    {
        SqlMapper<?> mapper = this.javaTypeSqlMapperMap.get(javaType);

        if (mapper == null)
        {
            // May be an anonymous implementation.
            Optional<SqlMapper<?>> optional = this.javaTypeSqlMapperMap.values().stream().filter(sc -> sc.getJavaType().isAssignableFrom(javaType)).findFirst();

            if (optional.isPresent())
            {
                mapper = optional.get();
                this.javaTypeSqlMapperMap.put(javaType, mapper);
            }
        }

        if (mapper == null)
        {
            LOGGER.warn("Class '{}' not mapped, using '{}' as default", javaType, FALLBACK_SQL_MAPPER.getClass().getSimpleName());

            mapper = FALLBACK_SQL_MAPPER;
            this.javaTypeSqlMapperMap.put(javaType, mapper);
        }

        return (SqlMapper<T>) mapper;
    }

    /**
     * @param <T> Type
     * @param jdbcType {@link JDBCType}
     * @return {@link SqlMapper}
     */
    @SuppressWarnings("unchecked")
    private <T> SqlMapper<T> findSqlMapper(final JDBCType jdbcType)
    {
        SqlMapper<?> mapper = this.jdbcTypeSqlMapperMap.get(jdbcType);

        if (mapper == null)
        {
            if (!FALLBACK_SQL_MAPPER.getSupportedJdbcTypes().contains(jdbcType))
            {
                LOGGER.warn("JDBCType '{}' not mapped, using '{}' as default", jdbcType, FALLBACK_SQL_MAPPER.getClass().getSimpleName());
            }

            mapper = FALLBACK_SQL_MAPPER;
            this.jdbcTypeSqlMapperMap.put(jdbcType, mapper);
        }

        return (SqlMapper<T>) mapper;
    }

    /**
     * @param <T> Type
     * @param javaType {@link Class}
     * @return {@link ObjectTransformer}
     */
    @SuppressWarnings("unchecked")
    private <T> ObjectTransformer<T> findTransformer(final Class<?> javaType)
    {
        ObjectTransformer<?> transformer = this.transformerMap.get(javaType);

        if (transformer == null)
        {
            // May be an anonymous implementation.
            Optional<ObjectTransformer<?>> optional = this.transformerMap.values().stream().filter(e -> e.getJavaType().isAssignableFrom(javaType)).findFirst();

            if (optional.isPresent())
            {
                transformer = optional.get();
                this.transformerMap.put(javaType, transformer);
            }
        }

        if (transformer == null)
        {
            LOGGER.warn("Class '{}' not mapped, using {} as default", javaType, FALLBACK_OBJECT_TRANSFORMER.getClass().getSimpleName());

            transformer = FALLBACK_OBJECT_TRANSFORMER;
            this.transformerMap.put(javaType, transformer);
        }

        return (ObjectTransformer<T>) transformer;
    }

    /**
     * Register a new {@link ObjectTransformer} for a Java-Class.
     *
     * @param transformer {@link ObjectTransformer}
     */
    public void register(final ObjectTransformer<?> transformer)
    {
        this.transformerMap.put(transformer.getJavaType(), transformer);
    }

    /**
     * Register a new {@link SqlMapper}.
     *
     * @param sqlMapper {@link SqlMapper}
     */
    public void register(final SqlMapper<?> sqlMapper)
    {
        sqlMapper.getSupportedJdbcTypes().forEach(jdbcType -> this.jdbcTypeSqlMapperMap.put(jdbcType, sqlMapper));

        this.javaTypeSqlMapperMap.put(sqlMapper.getJavaType(), sqlMapper);
    }
}
