package fr.sorbonne_u.broker.component;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.filters.value.JokerValueFilter;
import fr.sorbonne_u.messages.MessageTest;

/**
 * Unit tests for {@link ChannelBrokerInfos}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ChannelBrokerInfosTest {

    // -------------------------------------------------------------------------
    // CONSTRUCTOR
    // -------------------------------------------------------------------------

    @Test
    void testConstructorValid() {
        ChannelBrokerInfos channel1 = new ChannelBrokerInfos(
                "testChannel",
                ".*",
                "creatorURI"
        );
        assertEquals("creatorURI", channel1.getCreator());
        
        ChannelBrokerInfos channel2 = new ChannelBrokerInfos(
                "testChannel",
                ".*",
                null
        );
        assertNull(channel2.getCreator());
        
        ChannelBrokerInfos channel3 = new ChannelBrokerInfos(
                "testChannel",
                ".*",
                "creatorURI"
        );
        assertEquals("creatorURI", channel3.getCreator());
    }

    @Test
    void testConstructorInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> new ChannelBrokerInfos("", ".*", "creatorURI"));
        
        assertThrows(IllegalArgumentException.class,
                () -> new ChannelBrokerInfos(null, ".*", "creatorURI"));
        
        assertThrows(IllegalArgumentException.class,
                () -> new ChannelBrokerInfos("testChannel", null, "creatorURI"));
    }
    
    // -------------------------------------------------------------------------
    // SETUP
    // -------------------------------------------------------------------------

    private ChannelBrokerInfos channel;
    private MessageFilterI filterAcceptingAll;
    
    @BeforeEach
    void setUp() {
        channel = new ChannelBrokerInfos(
                "testChannel",
                "client.*",
                "creatorURI"
        );
        
        filterAcceptingAll = new MessageFilter(
                new PropertyFilterI[0], 
                new PropertiesFilterI[0], 
                new JokerTimeFilter()
        );
    }
    
    // -------------------------------------------------------------------------
    // SUBSCRIBERS
    // -------------------------------------------------------------------------

    @Test
    void testAddOrReplaceSubscriber() throws Exception {
        // Normal case - authorized client
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        assertTrue(channel.isSubscribed("client1"));
        
        // Filter replacement case
        MessageFilterI newFilter = new MessageFilter(
                new PropertyFilterI[0], 
                new PropertiesFilterI[0], 
                new GreaterOrEqualTimeFilter(Instant.parse("2026-04-24T12:00:00Z"))
        );
        channel.addOrReplaceSubscriber("client1", newFilter);
        assertTrue(channel.isSubscribed("client1"));
        
        // Unauthorized client
        assertThrows(UnauthorisedClientException.class,
                () -> channel.addOrReplaceSubscriber("admin1", filterAcceptingAll));
        
        // Invalid parameters
        assertThrows(IllegalArgumentException.class,
                () -> channel.addOrReplaceSubscriber("", filterAcceptingAll));
        assertThrows(IllegalArgumentException.class,
                () -> channel.addOrReplaceSubscriber(null, filterAcceptingAll));
        assertThrows(IllegalArgumentException.class,
                () -> channel.addOrReplaceSubscriber("client1", null));
    }

    @Test
    void testReplaceSubscriber() throws Exception {
        // Client not subscribed
        assertThrows(NotSubscribedChannelException.class,
                () -> channel.replaceSubscriber("client1", filterAcceptingAll));
        
        // Normal case - subscribe first, then replace
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        MessageFilterI newFilter = new MessageFilter(
                new PropertyFilterI[0], 
                new PropertiesFilterI[0], 
                new GreaterOrEqualTimeFilter(Instant.parse("2026-04-24T12:00:00Z"))
        );
        channel.replaceSubscriber("client1", newFilter);
        assertTrue(channel.isSubscribed("client1"));
        
        // Invalid parameters
        assertThrows(IllegalArgumentException.class,
                () -> channel.replaceSubscriber("", filterAcceptingAll));
        assertThrows(IllegalArgumentException.class,
                () -> channel.replaceSubscriber(null, filterAcceptingAll));
        assertThrows(IllegalArgumentException.class,
                () -> channel.replaceSubscriber("client1", null));
    }

    @Test
    void testIsSubscribed() throws Exception {
        assertFalse(channel.isSubscribed("client1"));
        
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        assertTrue(channel.isSubscribed("client1"));
        
        // Invalid parameters
        assertThrows(IllegalArgumentException.class,
                () -> channel.isSubscribed(""));
        assertThrows(IllegalArgumentException.class,
                () -> channel.isSubscribed(null));
    }

    @Test
    void testRemoveSubscriber() throws Exception {
        // Not subscribed
        assertThrows(NotSubscribedChannelException.class,
                () -> channel.removeSubscriber("client1"));
        
        // Normal case
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        channel.removeSubscriber("client1");
        assertFalse(channel.isSubscribed("client1"));
        
        // Invalid parameters
        assertThrows(IllegalArgumentException.class,
                () -> channel.removeSubscriber(""));
        assertThrows(IllegalArgumentException.class,
                () -> channel.removeSubscriber(null));
    }
    
    @Test
    void testGetSubscribers() throws Exception {
        String c1 = "client1";
        String c2 = "client2";
        
        // Empty list
        List<String> subs = channel.getSubscribers();
        assertTrue(subs.isEmpty());
        
        // Two subscribers
        channel.addOrReplaceSubscriber(c1, filterAcceptingAll);
        channel.addOrReplaceSubscriber(c2, filterAcceptingAll);
        
        subs = channel.getSubscribers();
        assertEquals(2, subs.size());
        assertTrue(subs.contains(c1) && subs.contains(c2));
        
        // Remove all
        channel.removeSubscriber(c1);
        channel.removeSubscriber(c2);
        
        subs = channel.getSubscribers();
        assertTrue(subs.isEmpty());
    }
    
    // -------------------------------------------------------------------------
    // AUTHORIZED USERS MANAGEMENT
    // -------------------------------------------------------------------------

    @Test
    void testModifyAuthorizedUsers() throws Exception {
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        channel.addOrReplaceSubscriber("client2", filterAcceptingAll);

        // Expand authorization - no one unsubscribed
        channel.modifyAuthorizedUsers("creatorURI", ".*");
        
        assertTrue(channel.isSubscribed("client1"));
        assertTrue(channel.isSubscribed("client2"));
        
        // Non-creator tries to modify
        assertThrows(UnauthorisedClientException.class,
                () -> channel.modifyAuthorizedUsers("client1", ".*"));
        
        // Invalid parameters
        assertThrows(IllegalArgumentException.class,
                () -> channel.modifyAuthorizedUsers("", ".*"));
        assertThrows(IllegalArgumentException.class,
                () -> channel.modifyAuthorizedUsers(null, ".*"));
        assertThrows(IllegalArgumentException.class,
                () -> channel.modifyAuthorizedUsers("creatorURI", null));
    }
    
    @Test
    void testModifyAuthorizedUsersWithRemoval() throws Exception {
        // First expand to allow "admin1" to subscribe
        channel.modifyAuthorizedUsers("creatorURI", ".*");
        
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        channel.addOrReplaceSubscriber("admin1", filterAcceptingAll);

        // Restrict to admin.* - client1 should be unsubscribed
        channel.modifyAuthorizedUsers("creatorURI", "admin.*");

        assertFalse(channel.isSubscribed("client1"));
        assertTrue(channel.isSubscribed("admin1"));
    }
    
    @Test
    void testIsAuthorized() {
        // Creator is always authorized
        assertTrue(channel.isAuthorized("creatorURI"));
        
        // Authorized by regex
        assertTrue(channel.isAuthorized("client1"));
        assertTrue(channel.isAuthorized("clientABC"));

        // Not authorized
        assertFalse(channel.isAuthorized("admin1"));
        assertFalse(channel.isAuthorized("user"));

        // Invalid parameters
        assertThrows(IllegalArgumentException.class, 
                () -> channel.isAuthorized(null));
        assertThrows(IllegalArgumentException.class, 
                () -> channel.isAuthorized(""));
    }
    
    // -------------------------------------------------------------------------
    // CREATOR MANAGEMENT
    // -------------------------------------------------------------------------
    
    @Test
    void testIsCreator() {
        assertTrue(channel.isCreator("creatorURI"));

        assertFalse(channel.isCreator("client1"));
        assertFalse(channel.isCreator("admin"));

        assertThrows(IllegalArgumentException.class, 
                () -> channel.isCreator(null));
        assertThrows(IllegalArgumentException.class, 
                () -> channel.isCreator(""));
    }

    @Test
    void testGetNbMaxBatch() {
        Map<String, ClientBrokerInfos> clientsInfos = new HashMap<>();
        ClientBrokerInfos.nbMaxChannelsStd = 2;
        ClientBrokerInfos.nbMaxChannelsPrem = 4;
        
        // Creator is null (broker created) - should return NB_MAX_BATCH_CHANNEL
        ChannelBrokerInfos brokerChannel = new ChannelBrokerInfos(
                "brokerChannel", ".*", null
        );
        assertEquals(ChannelBrokerInfos.NB_MAX_BATCH_CHANNEL, 
                brokerChannel.getNbMaxBatch(clientsInfos));
        
        // Creator unregistered - should return NB_MAX_BATCH_CHANNEL
        assertEquals(ChannelBrokerInfos.NB_MAX_BATCH_CHANNEL, 
                channel.getNbMaxBatch(clientsInfos));
        
        // Creator is FREE
        ClientBrokerInfos freeClient = new ClientBrokerInfos("creatorURI", RegistrationClass.FREE, null);
        clientsInfos.put("creatorURI", freeClient);
        assertEquals(ChannelBrokerInfos.NB_MAX_BATCH_CHANNEL, 
                channel.getNbMaxBatch(clientsInfos));
        
        // Creator is STANDARD
        ClientBrokerInfos stdClient = new ClientBrokerInfos("creatorURI", RegistrationClass.STANDARD, null);
        clientsInfos.put("creatorURI", stdClient);
        assertEquals(ChannelBrokerInfos.NB_MAX_BATCH_STD_CHANNEL, 
                channel.getNbMaxBatch(clientsInfos));
        
        // Creator is PREMIUM
        ClientBrokerInfos premClient = new ClientBrokerInfos("creatorURI", RegistrationClass.PREMIUM, null);
        clientsInfos.put("creatorURI", premClient);
        assertEquals(ChannelBrokerInfos.NB_MAX_BATCH_PREM_CHANNEL, 
                channel.getNbMaxBatch(clientsInfos));
        
        // Invalid parameter
        assertThrows(IllegalArgumentException.class,
                () -> channel.getNbMaxBatch(null));
    }

    // -------------------------------------------------------------------------
    // MESSAGES
    // -------------------------------------------------------------------------

    @Test
    void testLoadMessageAndUnload() {
        MessageI msg1 = new MessageTest.DummyMessage();
        MessageI msg2 = new MessageTest.DummyMessage();

        channel.loadMessage(msg1);
        channel.loadMessage(msg2);

        assertEquals(msg1, channel.unloadMessage());
        assertEquals(msg2, channel.unloadMessage());
        assertNull(channel.unloadMessage());
    }

    @Test
    void testLoadNullMessage() {
        assertThrows(IllegalArgumentException.class,
                () -> channel.loadMessage(null));
    }

    @Test
    void testTargetSubscribers() throws Exception {
        // Filter accepting only messages with "weight" property
        PropertyFilterI[] propFilters = new PropertyFilterI[1];
        propFilters[0] = new PropertyFilter("weight", new JokerValueFilter());
        MessageFilterI filterAcceptingAllWeight = new MessageFilter(
                propFilters, new PropertiesFilterI[0], new JokerTimeFilter()
        );
        
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        channel.addOrReplaceSubscriber("client2", filterAcceptingAllWeight);

        // Message with "height" property (not "weight")
        MessageI msg = new MessageTest.DummyMessage();
        msg.putProperty("height", 1.75);

        List<String> targets = channel.targetSubscribers(msg);

        // Only client1 accepts it (filter accepts all)
        assertEquals(1, targets.size());
        assertTrue(targets.contains("client1"));
        
        // Invalid parameter
        assertThrows(IllegalArgumentException.class,
                () -> channel.targetSubscribers(null));
    }

    // -------------------------------------------------------------------------
    // UNLOADING MANAGEMENT
    // -------------------------------------------------------------------------

    @Test
    void testTryNewUnloader() {
        int maxThreads = 10;
        int maxPerChannel = maxThreads / ChannelBrokerInfos.MAX_ACTIVE_UNLOADERS_RATIO;
        
        // Should succeed until we reach the limit
        for (int i = 0; i < maxPerChannel; i++) {
            assertTrue(channel.tryNewUnloader(maxThreads));
        }
        
        // Should fail once limit is reached
        assertFalse(channel.tryNewUnloader(maxThreads));
    }

    @Test
    void testUnloaderDone() {
        int maxThreads = 10;
        
        // Add some unloaders
        assertTrue(channel.tryNewUnloader(maxThreads));
        assertTrue(channel.tryNewUnloader(maxThreads));
        
        // Remove them
        channel.unloaderDone();
        channel.unloaderDone();
        
        // Should be able to add again
        assertTrue(channel.tryNewUnloader(maxThreads));
    }

    // -------------------------------------------------------------------------
    // EQUALITY
    // -------------------------------------------------------------------------

    @Test
    void testEqualsAndHashCode() {
        ChannelBrokerInfos other = new ChannelBrokerInfos(
                "testChannel", "admin.*", null
        );

        // Same channel name
        assertEquals(channel, other);
        assertEquals(channel.hashCode(), other.hashCode());
        
        // Same object
        assertEquals(channel, channel);
        assertEquals(channel.hashCode(), channel.hashCode());
        
        // Different channel name
        ChannelBrokerInfos different = new ChannelBrokerInfos(
                "differentChannel", "client.*", "creatorURI"
        );
        assertNotEquals(channel, different);
    }
}