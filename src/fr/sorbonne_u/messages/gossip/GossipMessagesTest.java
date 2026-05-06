package fr.sorbonne_u.messages.gossip;

import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.messages.MessageTest;
import fr.sorbonne_u.messages.gossip.GossipMessage.Action;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.ArrayList;

/**
 * Unit tests for {@link GossipMessage}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class GossipMessagesTest {

    // --------------------------------------------------------------------------
    // Constructor tests
    // --------------------------------------------------------------------------

    @Test
    public void testConstructor() {
        String emitterURI = "broker-1";
        Instant before = Instant.now();
        GossipMessage msg1 = new GossipMessage(emitterURI);
        Instant after = Instant.now();

        // Verify each attribute information
        assertEquals(emitterURI, msg1.emitterURI);
        assertNotNull(msg1.gossipMessageURI());
        assertTrue(msg1.gossipMessageURI().startsWith(emitterURI + "_"));
        assertEquals(Action.NOTHING, msg1.messageAction());
        assertNull(msg1.message());
        assertNotNull(msg1.timestamp());

        int expectedLength = emitterURI.length() + 1 + 20;
        assertEquals(expectedLength, msg1.gossipMessageURI().length());

        GossipMessage msg2 = new GossipMessage(emitterURI);
        assertNotEquals(msg1.gossipMessageURI(), msg2.gossipMessageURI());

        assertFalse(msg1.timestamp().isBefore(before));
        assertFalse(msg1.timestamp().isAfter(after));

        // Invalid parameters tests
        assertThrows(IllegalArgumentException.class, () -> new GossipMessage(null));
        assertThrows(IllegalArgumentException.class, () -> new GossipMessage(""));
    }

    // --------------------------------------------------------------------------
    // Copy tests
    // --------------------------------------------------------------------------

    @Test
    public void testCopyWithNewEmitterURI() {
        GossipMessage original = new GossipMessage("broker-1");

        GossipMessageI copy = original.copyWithNewEmitterURI("broker-2");

        // Check that the new gossip message content is as expected
        assertEquals("broker-2", ((GossipMessage) copy).emitterURI);
        assertEquals(original.gossipMessageURI(), copy.gossipMessageURI());
        assertEquals(original.messageAction(), ((GossipMessage) copy).messageAction());
    }

    @Test
    public void testCopyWithNewEmitterURIWithMessage() {
        GossipMessage original =
            GossipMessageFactory.createRegisterMessage(
                "broker-1", "port", RegistrationClass.FREE);

        GossipMessageI copy = original.copyWithNewEmitterURI("broker-2");

        // Check that the new gossip message content is as expected, 
        // with the same contained message
        assertNotNull(((GossipMessage) copy).message());
        assertEquals("broker-2", ((GossipMessage) copy).emitterURI);
        assertEquals(original.gossipMessageURI(), copy.gossipMessageURI());
        assertEquals(original.messageAction(), ((GossipMessage) copy).messageAction());
    }

    @Test
    public void testCopyErrors() {
        GossipMessage original = new GossipMessage("broker-1");

        // Invalid parameters tests
        assertThrows(IllegalArgumentException.class,
            () -> original.copyWithNewEmitterURI(null));
        assertThrows(IllegalArgumentException.class,
            () -> original.copyWithNewEmitterURI(""));
    }

    // --------------------------------------------------------------------------
    // Equality
    // --------------------------------------------------------------------------

    @Test
    void testEqualsAndHashCode() {
        GossipMessage original = new GossipMessage("broker-1");
        GossipMessageI copy = original.copyWithNewEmitterURI("broker-2");

        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
    }

    // --------------------------------------------------------------------------
    // Factory tests
    // --------------------------------------------------------------------------

    @Test
    public void testCreatePublishMessages() throws Exception {
        ArrayList<MessageI> messages = new ArrayList<>();
		messages.add(new MessageTest.DummyMessage());

        GossipMessage msg = GossipMessageFactory.createPublishMessages(
            "broker-1", "port", "channel", messages);

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.PUBLISH, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_PUBLISH.get(0)));
        assertEquals("channel",
            msg.message().getPropertyValue(GossipMessage.ARGS_PUBLISH.get(1)));
        assertNotNull(
            msg.message().getPropertyValue(GossipMessage.ARGS_PUBLISH.get(2)));
    }

    @Test
    public void testCreateRegisterMessage() throws Exception {
        GossipMessage msg = GossipMessageFactory.createRegisterMessage(
            "broker-1", "port", RegistrationClass.FREE);

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.REGISTER_CLIENT, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_REGISTER_CLIENT.get(0)));
        assertEquals(RegistrationClass.FREE,
            msg.message().getPropertyValue(GossipMessage.ARGS_REGISTER_CLIENT.get(1)));
    }

    @Test
    public void testCreateUnregisterMessage() throws Exception {
        GossipMessage msg = GossipMessageFactory.createUnregisterMessage(
            "broker-1", "port");

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.UNREGISTER_CLIENT, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_UNREGISTER_CLIENT.get(0)));
    }

    @Test
    public void testCreateCreateChannelMessage() throws Exception {
        GossipMessage msg = GossipMessageFactory.createCreateChannelMessage(
            "broker-1", "port", "channel", ".*");

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.CREATE_CHANNEL, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_CREATE_CHANNEL.get(0)));
        assertEquals("channel",
            msg.message().getPropertyValue(GossipMessage.ARGS_CREATE_CHANNEL.get(1)));
        assertEquals(".*",
            msg.message().getPropertyValue(GossipMessage.ARGS_CREATE_CHANNEL.get(2)));
    }

    @Test
    public void testCreateDestroyChannelMessage() throws Exception {
        GossipMessage msg = GossipMessageFactory.createDestroyChannelMessage(
            "broker-1", "port", "channel", true);

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.DESTROY_CHANNEL, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_DESTROY_CHANNEL.get(0)));
        assertEquals("channel",
            msg.message().getPropertyValue(GossipMessage.ARGS_DESTROY_CHANNEL.get(1)));
        assertEquals(true,
            msg.message().getPropertyValue(GossipMessage.ARGS_DESTROY_CHANNEL.get(2)));
    }

    @Test
    public void testCreateSubscribeClientMessage() throws Exception {
        MessageFilterI filter = new MessageFilter(
            new PropertyFilterI[0],
            new PropertiesFilterI[0],
            new JokerTimeFilter());

        GossipMessage msg = GossipMessageFactory.createSubscribeClientMessage(
            "broker-1", "port", "channel", filter);

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.SUBSCRIBE_CLIENT_CHANNEL, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(0)));
        assertEquals("channel",
            msg.message().getPropertyValue(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(1)));
        assertEquals(filter,
            msg.message().getPropertyValue(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(2)));
    }

    @Test
    public void testCreateUnsubscribeClientMessage() throws Exception {
        GossipMessage msg = GossipMessageFactory.createUnsubscribeClientMessage(
            "broker-1", "port", "channel");

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.UNSUBSCRIBE_CLIENT_CHANNEL, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_UNSUBSCRIBE_CLIENT_CHANNEL.get(0)));
        assertEquals("channel",
            msg.message().getPropertyValue(GossipMessage.ARGS_UNSUBSCRIBE_CLIENT_CHANNEL.get(1)));
    }

    @Test
    public void testCreateUpdateClientClassMessage() throws Exception {
        GossipMessage msg = GossipMessageFactory.createUpdateClientClassMessage(
            "broker-1", "port", RegistrationClass.PREMIUM);

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.UPDATE_CLIENT_SERVICE_CLASS, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_UPDATE_CLIENT_SERVICE_CLASS.get(0)));
        assertEquals(RegistrationClass.PREMIUM,
            msg.message().getPropertyValue(GossipMessage.ARGS_UPDATE_CLIENT_SERVICE_CLASS.get(1)));
    }

    @Test
    public void testCreateUpdateChannelAuthURIsMessage() throws Exception {
        GossipMessage msg = GossipMessageFactory.createUpdateChannelAuthURIsMessage(
            "broker-1", "port", "channel", ".*");

        // Ensures that the gossip message informations have correctly been set
        assertEquals(Action.UPDATE_CHANNEL_AUTHORIZED_URIS, msg.messageAction());

        assertEquals("port",
            msg.message().getPropertyValue(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(0)));
        assertEquals("channel",
            msg.message().getPropertyValue(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(1)));
        assertEquals(".*",
            msg.message().getPropertyValue(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(2)));
    }
}