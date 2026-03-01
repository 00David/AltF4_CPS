package fr.sorbonne_u.broker.components.infos.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import fr.sorbonne_u.broker.components.infos.ChannelBrokerInfos;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.filters.value.JokerValueFilter;
import fr.sorbonne_u.messages.Message;

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
    void testConstructorValid() { // also tests simple getters
    	ChannelBrokerInfos channel1 = new ChannelBrokerInfos(
                "testChannel",
                ".*",
                "creatorURI",
                false
        );
        assertEquals(".*", channel1.getAuthorizedUsers());
        assertEquals("creatorURI", channel1.getCreator());
        assertFalse(channel1.onlyPrivileged());
        
    	ChannelBrokerInfos channel2 = new ChannelBrokerInfos(
                "testChannel",
                ".*",
                null,
                false
        );
        assertEquals(".*", channel2.getAuthorizedUsers());
        assertEquals(null, channel2.getCreator());
        assertFalse(channel2.onlyPrivileged());
        
    	ChannelBrokerInfos channel3 = new ChannelBrokerInfos(
                "testChannel",
                ".*",
                "creatorURI",
                true
        );
        assertEquals(".*", channel3.getAuthorizedUsers());
        assertEquals("creatorURI", channel3.getCreator());
        assertTrue(channel3.onlyPrivileged());
    }

    @Test
    void testConstructorInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> new ChannelBrokerInfos("", ".*", "creatorURI", false)); // empty channel name
        
        assertThrows(IllegalArgumentException.class,
                () -> new ChannelBrokerInfos(null, ".*", "creatorURI", false)); // null channel name
        
        assertThrows(IllegalArgumentException.class,
                () -> new ChannelBrokerInfos("testChannel", null, "creatorURI", false)); // null authorized users
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
                "creatorURI",
                false
        );
        
        filterAcceptingAll = new MessageFilter(new PropertyFilterI[0], new PropertiesFilterI[0], new JokerTimeFilter());
    }
    
    // -------------------------------------------------------------------------
    // SUBSCRIBERS
    // -------------------------------------------------------------------------

    @Test
    void testAddSubscriber() { // also tests 'isSubscribed'
    	// Normal case
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        assertTrue(channel.isSubscribed("client1"));
        
        // Invalid parameters throwing an IllegalArgumentException for addOrReplaceSubscriber
        assertThrows(IllegalArgumentException.class,
                () -> channel.addOrReplaceSubscriber("", filterAcceptingAll)); // empty client URI
        assertThrows(IllegalArgumentException.class,
                () -> channel.addOrReplaceSubscriber(null, filterAcceptingAll)); // null client URI
        assertThrows(IllegalArgumentException.class,
                () -> channel.addOrReplaceSubscriber("client1", null)); // null filter
        
        // Invalid parameters throwing an IllegalArgumentException for isSubscribed
        assertThrows(IllegalArgumentException.class,
                () -> channel.isSubscribed("")); // empty client URI
        assertThrows(IllegalArgumentException.class,
                () -> channel.isSubscribed(null)); // null client URI
    }

    @Test
    void testRemoveSubscriber() {
    	// Normal case
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        channel.removeSubscriber("client1");
        assertFalse(channel.isSubscribed("client1"));
        
        // Invalid parameters throwing an IllegalArgumentException
        assertThrows(IllegalArgumentException.class,
                () -> channel.removeSubscriber("client2")); // not subscribed
        assertThrows(IllegalArgumentException.class,
                () -> channel.removeSubscriber("")); // empty client URI
        assertThrows(IllegalArgumentException.class,
                () -> channel.removeSubscriber(null)); // null client URI
    }
    
    @Test
    void testGetSubscribers() {
    	String c1 = "client1";
    	String c2 = "client2";
    	
        channel.addOrReplaceSubscriber(c1, filterAcceptingAll);
        channel.addOrReplaceSubscriber(c2, filterAcceptingAll);
        // channel with 2 subscibers

        List<String> subs = channel.getSubscribers();
        assertTrue(subs.contains(c1) && subs.contains(c2));
        
        
        channel.removeSubscriber(c1);
        channel.removeSubscriber(c2);
        // channel with 0 subscibers
        
        List<String> expectedList2 = new ArrayList<>();
        assertEquals(expectedList2, channel.getSubscribers());
    }
    
    // -------------------------------------------------------------------------
    // AUTHORIZED USERS MANAGEMENT
    // -------------------------------------------------------------------------

    @Test
    void testModifyAuthorizedUsers() {
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        channel.addOrReplaceSubscriber("client2", filterAcceptingAll);

        channel.modifyAuthorizedUsers(".*"); // No client unsubscribed
        
        assertTrue(channel.isSubscribed("client1"));
        assertTrue(channel.isSubscribed("client2"));
    }
    
    @Test
    void testModifyAuthorizedUsersWithRemoval() {
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        channel.addOrReplaceSubscriber("admin1", filterAcceptingAll);

        channel.modifyAuthorizedUsers("admin.*"); // client1 unsubscribed

        assertFalse(channel.isSubscribed("client1"));
        assertTrue(channel.isSubscribed("admin1"));
    }

    // -------------------------------------------------------------------------
    // MESSAGES
    // -------------------------------------------------------------------------

    @Test
    void testAddMessageAndGet() {
        MessageI msg1 = new Message();
        MessageI msg2 = new Message();

        channel.addMessage(msg1);
        channel.addMessage(msg2);

        assertEquals(msg1, channel.getMessage());
        assertEquals(msg2, channel.getMessage());
        assertNull(channel.getMessage()); // No more messages into the channel
    }

    @Test
    void testAddMessagesAndGet() {
    	MessageI msg1 = new Message();
    	MessageI msg2 = new Message();

        List<MessageI> msgs = new ArrayList<>(); msgs.add(msg1); msgs.add(msg2); 
        channel.addMessages(msgs);

        assertEquals(msg1, channel.getMessage());
        assertEquals(msg2, channel.getMessage());
        assertNull(channel.getMessage()); // No more messages into the channel
    }

    @Test
    void testAddNullMessages() {
    	// For AddMessage
        assertThrows(IllegalArgumentException.class,
                () -> channel.addMessage(null)); // message null
        
        // For AddMessages
        assertThrows(IllegalArgumentException.class,
                () -> channel.addMessages(null));  // messages null
        assertThrows(IllegalArgumentException.class,
                () -> channel.addMessages(new ArrayList<MessageI>()));  // messages empty
    }

    @Test
    void testTargetSubscribers() {
    	// Filter accepting only messages with "weight" property
    	PropertyFilterI[] propFilters = new PropertyFilterI[1];
    	propFilters[0] = new PropertyFilter("weight", new JokerValueFilter());
    	MessageFilterI filterAcceptingAllWeight = new MessageFilter(propFilters, new PropertiesFilterI[0], new JokerTimeFilter());
    	
        channel.addOrReplaceSubscriber("client1", filterAcceptingAll);
        channel.addOrReplaceSubscriber("client2", filterAcceptingAllWeight);

        // Message with "height" property
        MessageI msg = new Message();
        msg.putProperty("height", 1.75);

        List<String> targets = channel.targetSubscribers(msg);

        // Only the client1 is targeted by the message
        assertEquals(1, targets.size());
        assertTrue(targets.contains("client1"));
       
        
        // Invalid parameter : throws an IllegalArgumentException
        assertThrows(IllegalArgumentException.class,
                () -> channel.targetSubscribers(null));
    }

    // -------------------------------------------------------------------------
    // EQUALITY
    // -------------------------------------------------------------------------

    @Test
    void testEqualsAndHashCode() {
        ChannelBrokerInfos other =
                new ChannelBrokerInfos("testChannel", "admin.*", null, false); // authorized users : admin.* != client.*

        // Equality ok because of same channel name "testChannel"
        assertEquals(channel, other);
        assertEquals(channel.hashCode(), other.hashCode());
        
        // Equality ok because of same channel
        assertEquals(channel, channel);
        assertEquals(channel.hashCode(), channel.hashCode());
    }
}
