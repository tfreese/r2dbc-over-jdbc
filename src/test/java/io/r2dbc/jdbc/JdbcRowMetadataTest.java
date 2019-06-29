/*
 * Copyright 2017-2018 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed
 * to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

package io.r2dbc.jdbc;

import static io.r2dbc.spi.Nullability.NULLABLE;
import static org.assertj.core.api.Assertions.assertThat;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Thomas Freese
 */
// @MockitoSettings(strictness = Strictness.LENIENT)
final class JdbcRowMetadataTest
{
    /**
     *
     */
    private final List<JdbcColumnMetadata> columnMetadatas =
            Arrays.asList(new JdbcColumnMetadata("TEST-NAME-1", JDBCType.OTHER.getVendorTypeNumber(), NULLABLE, 100, 500),
                    new JdbcColumnMetadata("TEST-NAME-2", JDBCType.OTHER.getVendorTypeNumber(), NULLABLE, 300, 600));

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
    public void beforeEach() throws SQLException
    {
        when(this.resultSet.getMetaData()).thenReturn(this.resultSetMetaData);

        when(this.resultSetMetaData.getColumnCount()).thenReturn(this.columnMetadatas.size());

        for (int i = 0; i < this.columnMetadatas.size(); i++)
        {
            when(this.resultSetMetaData.getColumnLabel(i + 1)).thenReturn(this.columnMetadatas.get(i).getName());
            when(this.resultSetMetaData.getColumnType(i + 1)).thenReturn((int) this.columnMetadatas.get(i).getNativeTypeMetadata());
            when(this.resultSetMetaData.isNullable(i + 1)).thenReturn(ResultSetMetaData.columnNullable);
            when(this.resultSetMetaData.getPrecision(i + 1)).thenReturn(this.columnMetadatas.get(i).getPrecision());
            when(this.resultSetMetaData.getScale(i + 1)).thenReturn(this.columnMetadatas.get(i).getScale());
        }
    }

    /**
     *
     */
    @Test
    void constructorNoColumnMetadata()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcRowMetadata(null)).withMessage("columnMetaDatas must not be null");
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void getColumnMetadataIndex() throws SQLException
    {
        assertThat(JdbcRowMetadata.of(this.resultSet).getColumnMetadata(0)).isEqualTo(this.columnMetadatas.get(0));
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void getColumnMetadataInvalidName() throws SQLException
    {
        assertThat(JdbcRowMetadata.of(this.resultSet).getColumnMetadata("test-name-3")).isEqualTo(null);
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void getColumnMetadataName() throws SQLException
    {
        assertThat(JdbcRowMetadata.of(this.resultSet).getColumnMetadata("TEST-NAME-2")).isEqualTo(this.columnMetadatas.get(1));
    }

    /**
     *
     */
    @Test
    void getColumnMetadataNoIdentifier()
    {
        assertThatNullPointerException().isThrownBy(() -> JdbcRowMetadata.of(this.resultSet).getColumnMetadata(null))
                .withMessage("identifier must not be null");
    }

    /**
     *
     */
    @Test
    void getColumnMetadataWrongIdentifierType()
    {
        Object identifier = new Object();

        assertThatIllegalArgumentException().isThrownBy(() -> JdbcRowMetadata.of(this.resultSet).getColumnMetadata(identifier))
                .withMessage("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier.toString());
    }

    /**
     *
     */
    @Test
    void toRowMetadataNoResultSet()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcRowMetadata(null)).withMessage("columnMetaDatas must not be null");
    }
}
