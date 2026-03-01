package fr.sorbonne_u.broker.components.infos.tests;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import fr.sorbonne_u.broker.components.infos.ClientBrokerInfos;
import fr.sorbonne_u.broker.components.infos.exceptions.NegativeRemainingChannelsException;
import fr.sorbonne_u.broker.components.infos.exceptions.CannotIncreaseRemainingChannelsException;
import fr.sorbonne_u.broker.components.infos.exceptions.ChannelNumberNotInitializedException;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Unit tests for {@link ClientBrokerInfos}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientBrokerInfosTest {
	
    // -------------------------------------------------------------------------
    // CONSTRUCTOR
    // -------------------------------------------------------------------------

    @Test
    void testConstructorValid() { // also tests simple getters
    	
    	// number of max channels for standard and premium clients have already been initialized in the 'setUp' method
    	
    	ClientBrokerInfos client1 = new ClientBrokerInfos("client1URI", RegistrationClass.FREE, null);
        assertEquals(RegistrationClass.FREE, client1.getRegistrationClass());
        assertEquals(null, client1.getOutboundPort());
        
    	ClientBrokerInfos client2 = new ClientBrokerInfos("client2URI", RegistrationClass.STANDARD, null);
        assertEquals(RegistrationClass.STANDARD, client2.getRegistrationClass());
        assertEquals(null, client2.getOutboundPort());
        
    	ClientBrokerInfos client3 = new ClientBrokerInfos("client3URI", RegistrationClass.PREMIUM, null);
        assertEquals(RegistrationClass.PREMIUM, client3.getRegistrationClass());
        assertEquals(null, client3.getOutboundPort());
    }

    @Test
    void testConstructorInvalid() {
    	
    	// number of max channels for standard and premium clients have already been initialized in the 'setUp' method

        assertThrows(IllegalArgumentException.class,
                () -> new ClientBrokerInfos("", RegistrationClass.FREE, null));  // empty client URU

        assertThrows(IllegalArgumentException.class,
                () -> new ClientBrokerInfos(null, RegistrationClass.FREE, null)); // null client URU

        assertThrows(IllegalArgumentException.class,
                () -> new ClientBrokerInfos("clientURI", null, null)); // null registration class
    }

    @Test
    void testConstructorInvalidMaxChannels() {
    	
    	// Considers number of max channels by default, not initialized
        ClientBrokerInfos.nbMaxChannelsStd = -1;
        ClientBrokerInfos.nbMaxChannelsPrem = -1;

        assertThrows(ChannelNumberNotInitializedException.class,
                () -> new ClientBrokerInfos("clientURI", RegistrationClass.FREE, null));
    }
    

    private ClientBrokerInfos client;

    // -------------------------------------------------------------------------
    // SETUP
    // -------------------------------------------------------------------------

    @BeforeEach
    void setUp() {
    	// Set max channels, same as inside a Broker constructor
        ClientBrokerInfos.nbMaxChannelsStd = 2;
        ClientBrokerInfos.nbMaxChannelsPrem = 5;

        client = new ClientBrokerInfos(
                "clientURI",
                RegistrationClass.STANDARD,
                null // No port associated, it would be too complicated considering assertions in its setup
        );
    }

    // -------------------------------------------------------------------------
    // REGISTRATION CLASS
    // -------------------------------------------------------------------------

    @Test
    void testSetRegistrationClass() {
    	client.setRegistrationClass(RegistrationClass.PREMIUM);
        assertEquals(RegistrationClass.PREMIUM, client.getRegistrationClass());

        // Invalid parameter : throws an IllegalArgumentException
        assertThrows(IllegalArgumentException.class,
                () -> client.setRegistrationClass(null));
    }

    // -------------------------------------------------------------------------
    // REMAINING CHANNELS
    // -------------------------------------------------------------------------

    @Test
    void testDecreaseRemainingChannels() {
        assertTrue(client.canCreateChannel()); // Client is STANDARD, has max 2 channels

        client.decreaseRemainingChannels();
        client.decreaseRemainingChannels();

        assertFalse(client.canCreateChannel()); // Now the client has 0 channels remaining

        // If we try to decrease while the client cannot create a channel anymore : throws a NegativeRemainingChannelsException
        assertThrows(NegativeRemainingChannelsException.class,
                () -> client.decreaseRemainingChannels());
    }

    @Test
    void testIncreaseRemainingChannels() {
    	client.decreaseRemainingChannels();
        client.decreaseRemainingChannels();

        assertFalse(client.canCreateChannel()); // Now the client has 0 channels remaining

        client.increaseRemainingChannels();

        assertTrue(client.canCreateChannel()); // Now the client has 1 channel remaining, can create
    }

    @Test
    void testFreeClientCannotIncreaseRemainingChannels() {
        ClientBrokerInfos freeClient = new ClientBrokerInfos("freeClientURI", RegistrationClass.FREE, null);
        
        assertThrows(CannotIncreaseRemainingChannelsException.class,
                () -> freeClient.increaseRemainingChannels()); // A free client cannot create any channel
    }

    // -------------------------------------------------------------------------
    // EQUALITY
    // -------------------------------------------------------------------------

    @Test
    void testEqualsAndHashCode() {
        ClientBrokerInfos other = 
        		new ClientBrokerInfos("clientURI", RegistrationClass.PREMIUM, null);

        // Equality ok because of same client URI "clientURI"
        assertEquals(client, other);
        assertEquals(client.hashCode(), other.hashCode());

        // Equality ok because of same client
        assertEquals(client, client);
        assertEquals(client.hashCode(), client.hashCode());
    }
}