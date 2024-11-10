// Created: 14.06.2019
package io.r2dbc.jdbc;

import static io.r2dbc.spi.Nullability.NULLABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.jdbc.codecs.DefaultCodecs;
import io.r2dbc.spi.ColumnMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Thomas Freese
 */
// @MockitoSettings(strictness = Strictness.LENIENT)
final class JdbcRowMetadataTest {
    @BeforeAll
    static void beforeAll() {
        // this.columnMetadatas = Arrays.asList(new JdbcColumnMetadata("TEST-NAME-1", JDBCType.OTHER, NULLABLE, 100, 500),
        // new JdbcColumnMetadata("TEST-NAME-2", JDBCType.OTHER, NULLABLE, 300, 600));
    }

    private final Codecs codecs = new DefaultCodecs();

    private final List<ColumnMetadata> columnMetadatas = Arrays.asList(new JdbcColumnMetadata("TEST-NAME-1", 0, Object.class, JDBCType.OTHER, NULLABLE, 100, 500),
            new JdbcColumnMetadata("TEST-NAME-2", 1, Object.class, JDBCType.OTHER, NULLABLE, 300, 600));

    private final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);

    private final ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class, RETURNS_SMART_NULLS);

    @BeforeEach
    void beforeEach() throws SQLException {
        when(this.resultSet.getMetaData()).thenReturn(this.resultSetMetaData);

        when(this.resultSetMetaData.getColumnCount()).thenReturn(this.columnMetadatas.size());

        for (int i = 0; i < this.columnMetadatas.size(); i++) {
            when(this.resultSetMetaData.getColumnLabel(i + 1)).thenReturn(this.columnMetadatas.get(i).getName());
            when(this.resultSetMetaData.getColumnType(i + 1)).thenReturn(((JDBCType) this.columnMetadatas.get(i).getNativeTypeMetadata()).getVendorTypeNumber());
            when(this.resultSetMetaData.isNullable(i + 1)).thenReturn(ResultSetMetaData.columnNullable);
            when(this.resultSetMetaData.getPrecision(i + 1)).thenReturn(this.columnMetadatas.get(i).getPrecision());
            when(this.resultSetMetaData.getScale(i + 1)).thenReturn(this.columnMetadatas.get(i).getScale());
        }
    }

    @Test
    void testConstructorNoColumnMetadata() {
        assertThatNullPointerException().isThrownBy(() -> new JdbcRowMetadata(null)).withMessage("columnMetaDatas must not be null");
    }

    @Test
    void testGetColumnMetadataIndex() throws SQLException {
        assertThat(JdbcRowMetadata.of(this.resultSet, this.codecs).getColumnMetadata(0)).isEqualTo(this.columnMetadatas.getFirst());
    }

    @Test
    void testGetColumnMetadataInvalidName() {
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> JdbcRowMetadata.of(this.resultSet, this.codecs).getColumnMetadata("test-name-3"));
    }

    @Test
    void testGetColumnMetadataName() throws SQLException {
        assertThat(JdbcRowMetadata.of(this.resultSet, this.codecs).getColumnMetadata("TEST-NAME-2")).isEqualTo(this.columnMetadatas.get(1));
    }

    @Test
    void testGetColumnMetadataNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> JdbcRowMetadata.of(this.resultSet, this.codecs).getColumnMetadata(null)).withMessage("name is null");
    }

    @Test
    void testGetColumnMetadataWrongIdentifierType() {
        final String identifier = "-";

        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> JdbcRowMetadata.of(this.resultSet, this.codecs).getColumnMetadata(identifier));
    }

    @Test
    void testToRowMetadataNoResultSet() {
        assertThatNullPointerException().isThrownBy(() -> new JdbcRowMetadata(null)).withMessage("columnMetaDatas must not be null");
    }
}
