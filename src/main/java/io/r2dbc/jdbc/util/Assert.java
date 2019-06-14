/**
 * Created: 14.06.2019
 */

package io.r2dbc.jdbc.util;

import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public final class Assert
{
    /**
     * @param value Object
     * @param message String
     * @throws SQLException Falls was schief geht.
     */
    public static void assertNotNull(final Object value, final String message) throws SQLException
    {
        if (value == null)
        {
            throw new SQLException(message);
        }
    }

    /**
     * @param value String
     * @param message String
     * @throws SQLException Falls was schief geht.
     */
    public static void assertNotNullOrBlank(final String value, final String message) throws SQLException
    {
        if ((value == null) || value.isBlank())
        {
            throw new SQLException(message);
        }
    }

    /**
     * Erstellt ein neues {@link Assert} Object.
     */
    private Assert()
    {
        super();
    }
}
