package org.apache.ignite.client;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.ssl.SslContextFactory.DFLT_KEY_ALGORITHM;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;

public class ClientSslTest extends GridCommonAbstractTest {

    /**
     * @return {@link ClientConfiguration} that can be used to start a thin client.
     */
    protected ClientConfiguration createValidClientSslConfiguration() {
        return new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setSslMode(SslMode.REQUIRED)
            .setSslClientCertificateKeyStorePath(GridTestUtils.keyStorePath("client"))
            .setSslClientCertificateKeyStoreType(DFLT_STORE_TYPE)
            .setSslClientCertificateKeyStorePassword("123456")
            .setSslTrustCertificateKeyStorePath(GridTestUtils.keyStorePath("trustone"))
            .setSslTrustCertificateKeyStoreType(DFLT_STORE_TYPE)
            .setSslTrustCertificateKeyStorePassword("123456")
            .setSslKeyAlgorithm(DFLT_KEY_ALGORITHM);
    }

    protected IgniteConfiguration createClusterNode() throws Exception {
        IgniteConfiguration cfg = Config.getServerConfiguration();

        ClientConnectorConfiguration clientConnectorCfg = new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
            .setUseIgniteSslContextFactory(false)
            .setSslContextFactory(serverSSLFactory());
        cfg.setClientConnectorConfiguration(clientConnectorCfg);

        return cfg;
    }

    /**
     * @return SSL context factory to use on server nodes for communication between nodes in a cluster.
     */
    private Factory<SSLContext> serverSSLFactory() {
        return GridTestUtils.sslTrustedFactory("server", "trustone");
    }

    @Test
    public void testThinClientWithEnabledSslConnectToSecuredGrid() throws Exception {
        IgniteClient client = null;
        String cacheName = "sslTestsCache";

        try (Ignite ignite = Ignition.start(createClusterNode())) {
            client = Ignition.startClient(createValidClientSslConfiguration());
            client.getOrCreateCache(cacheName);
        }
        catch (ClientException ex) {
            fail("Failed to start thin Java client: " + ex.getMessage());
        }
        finally {
            if (client != null) {
                client.close();
            }

            IgniteCache<Object, Object> clientCache = grid(0).cache(cacheName);
            if (clientCache != null) {
                clientCache.destroy();
            }
        }
    }

    @Test
    public void testThinClientWithoutSSlConnectToSecuredGrid() throws Exception {
        IgniteClient client = null;
        boolean isFailed = false;
        String cacheName = "sslTestsCache";

        try (Ignite ignite = Ignition.start(createClusterNode())) {
            ClientConfiguration configuration = new ClientConfiguration()
                .setAddresses(Config.SERVER);

            client = Ignition.startClient(configuration);
            client.getOrCreateCache(cacheName);
        }
        catch (ClientException ex) {
            isFailed = true;
        }
        finally {
            if (client != null) {
                client.close();
            }
            assertTrue("Client without SSL enabled connected to cluster with SSL enabled", isFailed);
        }
    }

    @Test
    public void testThinClientWithEnabledSslConnectToNonSecuredGrid() throws Exception {
        IgniteClient client = null;
        boolean isFailed = false;
        String cacheName = "sslTestsCache";

        try (Ignite ignite = Ignition.start(Config.getServerConfiguration())) {
            client = Ignition.startClient(createValidClientSslConfiguration());
            client.getOrCreateCache(cacheName);
        }
        catch (ClientException ex) {
            isFailed = true;
        }
        finally {
            if (client != null) {
                client.close();
            }
            assertTrue("Client with SSL enabled connected to cluster without SSL enabled", isFailed);
        }
    }

}
