// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * @author Thomas Freese
 */
public class DateCodec extends AbstractCodec<Date> {
    public DateCodec() {
        super(Date.class, JDBCType.DATE, JDBCType.TIME, JDBCType.TIMESTAMP);
    }

    @Override
    public Date mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final java.sql.Date sqlDate = resultSet.getDate(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        // java.sql.Date -> java.util.Date
        return new Date(sqlDate.getTime());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M> M mapTo(final Class<M> javaType, final Date value) {
        if (value == null) {
            return null;
        }

        if (getJavaType().equals(javaType) || Object.class.equals(javaType)) {
            return (M) value;
        }
        else if (LocalDate.class.equals(javaType)) {
            final LocalDate localDate = LocalDate.ofInstant(value.toInstant(), ZoneId.systemDefault());

            return (M) localDate;
        }
        else if (LocalDateTime.class.equals(javaType)) {
            final LocalDateTime localDateTime = value.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();

            return (M) localDateTime;
        }
        else if (LocalTime.class.equals(javaType)) {
            final LocalTime localTime = LocalTime.ofInstant(value.toInstant(), ZoneId.systemDefault());

            return (M) localTime;
        }
        else if (String.class.equals(javaType)) {
            final String formatted = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", value);

            return (M) formatted;
        }

        throw throwCanNotMapException(value);
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Date value) throws SQLException {
        final java.sql.Date sqlDate = new java.sql.Date(value.getTime());

        preparedStatement.setDate(parameterIndex, sqlDate);
    }
}
