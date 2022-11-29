// Created: 06.04.2021
package io.r2dbc.jdbc.testKit;

import io.r2dbc.jdbc.util.DbServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/**
 * @author Thomas Freese
 */
//@Disabled("io.r2dbc.spi.test.TestKit.TestStatement verwendet für H2 ungültige Spaltennamen")
public class H2TestKit extends AbstractTestKit
{
    @RegisterExtension
    static final DbServerExtension SERVER = new DbServerExtension(EmbeddedDatabaseType.H2);

    /**
     * @see io.r2dbc.jdbc.testKit.AbstractTestKit#getServer()
     */
    @Override
    DbServerExtension getServer()
    {
        return SERVER;
    }
}
