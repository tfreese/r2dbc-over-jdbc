// Created: 14.06.2019
package io.r2dbc.jdbc;

import static io.r2dbc.spi.Nullability.NULLABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.r2dbc.spi.ColumnMetadata;

/**
 * @author Thomas Freese
 */
// @MockitoSettings(strictness = Strictness.LENIENT)
final class JdbcRowMetadataTest
{
    /**
     *
     */
    private final List<JdbcColumnMetadata> columnMetadatas = Arrays.asList(new JdbcColumnMetadata("TEST-NAME-1", JDBCType.OTHER, NULLABLE, 100, 500),
            new JdbcColumnMetadata("TEST-NAME-2", JDBCType.OTHER, NULLABLE, 300, 600));

    /**
     *
     */
    private final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);

    /**
     *
     */
    private final ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class, RETURNS_SMART_NULLS);

    /**
     * @throws SQLException Falls was schief geht.
     */
    @BeforeEach
    void beforeEach() throws SQLException
    {
        when(this.resultSet.getMetaData()).thenReturn(this.resultSetMetaData);

        when(this.resultSetMetaData.getColumnCount()).thenReturn(this.columnMetadatas.size());

        for (int i = 0; i < this.columnMetadatas.size(); i++)
        {
            when(this.resultSetMetaData.getColumnLabel(i + 1)).thenReturn(this.columnMetadatas.get(i).getName());
            when(this.resultSetMetaData.getColumnType(i + 1))
                    .thenReturn(((JDBCType) this.columnMetadatas.get(i).getNativeTypeMetadata()).getVendorTypeNumber());
            when(this.resultSetMetaData.isNullable(i + 1)).thenReturn(ResultSetMetaData.columnNullable);
            when(this.resultSetMetaData.getPrecision(i + 1)).thenReturn(this.columnMetadatas.get(i).getPrecision());
            when(this.resultSetMetaData.getScale(i + 1)).thenReturn(this.columnMetadatas.get(i).getScale());
        }
    }

    /**
     *
     */
    @Test
    void testConstructorNoColumnMetadata()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcRowMetadata(null)).withMessage("columnMetaDatas must not be null");
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testGetColumnMetadataIndex() throws SQLException
    {
        assertThat(JdbcRowMetadata.of(this.resultSet).getColumnMetadata(0)).isEqualTo(this.columnMetadatas.get(0));
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testGetColumnMetadataInvalidName() throws SQLException
    {
        assertThat(JdbcRowMetadata.of(this.resultSet).getColumnMetadata("test-name-3")).isNull();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testGetColumnMetadataName() throws SQLException
    {
        assertThat(JdbcRowMetadata.of(this.resultSet).getColumnMetadata("TEST-NAME-2")).isEqualTo(this.columnMetadatas.get(1));
    }

    /**
     *
     */
    @Test
    void testGetColumnMetadataNoIdentifier()
    {
        assertThatNullPointerException().isThrownBy(() -> JdbcRowMetadata.of(this.resultSet).getColumnMetadata(null)).withMessage("name required");
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testGetColumnMetadataWrongIdentifierType() throws SQLException
    {
        String identifier = "-";

        // assertThatIllegalArgumentException().isThrownBy(() -> JdbcRowMetadata.of(this.resultSet).getColumnMetadata(identifier))
        // .withMessage("Column identifier '%s' does not exist", identifier.toString());

        ColumnMetadata columnMetadata = JdbcRowMetadata.of(this.resultSet).getColumnMetadata(identifier);
        assertNull(columnMetadata);
    }

    /**
     *
     */
    @Test
    void testToRowMetadataNoResultSet()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcRowMetadata(null)).withMessage("columnMetaDatas must not be null");
    }
}
