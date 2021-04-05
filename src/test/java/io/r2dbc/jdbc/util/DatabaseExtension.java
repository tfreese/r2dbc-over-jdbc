// Created: 05.04.2021
package io.r2dbc.jdbc.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/**
 * Dient zum starten und runterfahtren der DB-Instanzen.
 *
 * @author Thomas Freese
 */
public class DatabaseExtension implements BeforeAllCallback, AfterAllCallback// , ArgumentsProvider
{
    /**
     *
     */
    private final List<DBServerExtension> servers = new ArrayList<>();

    /**
     * Die Junit-{@link Extension} braucht zwingend einen Default-Constructor !
     */
    public DatabaseExtension()
    {
        super();

        this.servers.add(new DBServerExtension(EmbeddedDatabaseType.HSQL));
        this.servers.add(new DBServerExtension(EmbeddedDatabaseType.H2));
        this.servers.add(new DBServerExtension(EmbeddedDatabaseType.DERBY));
    }

    /**
     * @see org.junit.jupiter.api.extension.AfterAllCallback#afterAll(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void afterAll(final ExtensionContext context) throws Exception
    {
        for (DBServerExtension server : this.servers)
        {
            server.afterAll(context);
        }
    }

    /**
     * @see org.junit.jupiter.api.extension.BeforeAllCallback#beforeAll(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void beforeAll(final ExtensionContext context) throws Exception
    {
        for (DBServerExtension server : this.servers)
        {
            server.beforeAll(context);
        }
    }

    /**
     * @return {@link List}<DBServerExtension>
     */
    public List<DBServerExtension> getServers()
    {
        return this.servers;
    }

    // /**
    // * @see org.junit.jupiter.params.provider.ArgumentsProvider#provideArguments(org.junit.jupiter.api.extension.ExtensionContext)
    // */
    // @Override
    // public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception
    // {
    // return getServers().stream().map(server -> Arguments.of(server.getDatabaseType(), server));
    // }
}
