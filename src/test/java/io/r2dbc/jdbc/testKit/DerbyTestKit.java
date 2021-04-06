// Created: 06.04.2021
package io.r2dbc.jdbc.testKit;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import io.r2dbc.jdbc.util.DBServerExtension;

/**
 * @author Thomas Freese
 */
public class DerbyTestKit extends AbstractTestKit
{
    /**
    *
    */
    @RegisterExtension
    static final DBServerExtension SERVER = new DBServerExtension(EmbeddedDatabaseType.DERBY);

    /**
     * @see io.r2dbc.jdbc.testKit.AbstractTestKit#getServer()
     */
    @Override
    DBServerExtension getServer()
    {
        return SERVER;
    }
}
