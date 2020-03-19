/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.RowMetadata;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcRowMetadata implements RowMetadata
{
    /**
     * @param resultSet {@link ResultSet}
     * @return {@link List}
     * @throws SQLException Falls was schief geht.
     */
    public static JdbcRowMetadata of(final ResultSet resultSet) throws SQLException
    {
        if (resultSet == null)
        {
            return new JdbcRowMetadata(Collections.emptyList());
        }

        ResultSetMetaData metaData = resultSet.getMetaData();
        List<JdbcColumnMetadata> list = new ArrayList<>();

        for (int c = 1; c <= metaData.getColumnCount(); c++)
        {
            String name = metaData.getColumnLabel(c).toUpperCase();
            int sqlType = metaData.getColumnType(c);
            JDBCType jdbcType = JDBCType.valueOf(sqlType);

            Nullability nullability = null;

            switch (metaData.isNullable(c))
            {
                case ResultSetMetaData.columnNoNulls:
                    nullability = Nullability.NON_NULL;
                    break;

                case ResultSetMetaData.columnNullable:
                    nullability = Nullability.NULLABLE;
                    break;

                default:
                    nullability = Nullability.UNKNOWN;
                    break;
            }

            int precision = metaData.getPrecision(c);
            int scale = metaData.getScale(c);

            list.add(new JdbcColumnMetadata(name, jdbcType, nullability, precision, scale));
        }

        return new JdbcRowMetadata(list);
    }

    /**
     *
     */
    private final Map<String, JdbcColumnMetadata> columnMetaDataByName;

    /**
     *
     */
    private final List<JdbcColumnMetadata> columnMetaDatas;

    /**
     * Erstellt ein neues {@link JdbcRowMetadata} Object.
     *
     * @param columnMetaDatas {@link ResultSet}
     */
    public JdbcRowMetadata(final List<JdbcColumnMetadata> columnMetaDatas)
    {
        super();

        this.columnMetaDatas = Objects.requireNonNull(columnMetaDatas, "columnMetaDatas must not be null");

        this.columnMetaDataByName = this.columnMetaDatas.stream()
                .collect(Collectors.toMap(cmd -> cmd.getName().toUpperCase(), Function.identity(), (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnMetadata(int)
     */
    @Override
    public ColumnMetadata getColumnMetadata(final int index)
    {
        return this.columnMetaDatas.get(index);
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnMetadata(java.lang.String)
     */
    @Override
    public ColumnMetadata getColumnMetadata(final String name)
    {
        Objects.requireNonNull(name, "name required");

        return this.columnMetaDataByName.get(name.toUpperCase());
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnMetadatas()
     */
    @Override
    public Iterable<ColumnMetadata> getColumnMetadatas()
    {
        return List.copyOf(this.columnMetaDatas);
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnNames()
     */
    @Override
    public Collection<String> getColumnNames()
    {
        // return this.columnMetaDatas.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        return new LinkedHashSet<>(this.columnMetaDataByName.keySet());
    }
}
