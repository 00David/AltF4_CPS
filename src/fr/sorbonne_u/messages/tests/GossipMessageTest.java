package fr.sorbonne_u.messages.tests;

import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.messages.GossipMessage;
import fr.sorbonne_u.messages.GossipMessage.Action;
import fr.sorbonne_u.messages.Message;

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
public class GossipMessageTest {
	
	// --------------------------------------------------------------------------
	// Constructor tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testConstructor() {
		String emitterURI = "broker-1";
		Instant before = Instant.now();
		GossipMessage msg1 = new GossipMessage(emitterURI);
		Instant after = Instant.now();
		
		// attributes initialization
		assertEquals(emitterURI, msg1.emitterURI);
		assertNotNull(msg1.messageURI);
		assertTrue(msg1.messageURI.startsWith(emitterURI + "_"));
		assertEquals(Action.NOTHING, msg1.messageAction());
		assertNull(msg1.message());
		assertNotNull(msg1.timestamp());
		
		// messageURI format: emitterURI + "_" + 20 random characters
		int expectedLength = emitterURI.length() + 1 + 20;
		assertEquals(expectedLength, msg1.messageURI.length());
		
		// Uniqueness
		GossipMessage msg2 = new GossipMessage(emitterURI);
		assertNotEquals(msg1.messageURI, msg2.messageURI);
		
		// Timestamp check
		assertFalse(msg1.timestamp().isBefore(before));
		assertFalse(msg1.timestamp().isAfter(after));
		
		// Invalid instantiations
		assertThrows(IllegalArgumentException.class, () -> new GossipMessage(null));
		assertThrows(IllegalArgumentException.class, () -> new GossipMessage(""));
	}
	
	// --------------------------------------------------------------------------
	// Getter tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testGossipMessageURI() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		assertEquals(gossipMessage.messageURI, gossipMessage.gossipMessageURI());
	}
	
	@Test
	public void testTimestamp() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		assertEquals(gossipMessage.timestamp, gossipMessage.timestamp());
	}
	
	@Test
	public void testMessageActionDefault() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		assertEquals(Action.NOTHING, gossipMessage.messageAction());
	}
	
	@Test
	public void testMessageDefault() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		assertNull(gossipMessage.message());
	}
	
	// --------------------------------------------------------------------------
	// Copy tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testCopyWithNewEmitterURI() {
		GossipMessage original = new GossipMessage("broker-1");
		String newEmitterURI = "broker-2";
		
		GossipMessageI copy = original.copyWithNewEmitterURI(newEmitterURI);
		
		assertEquals(newEmitterURI, ((GossipMessage) copy).emitterURI);
		assertEquals(original.messageURI, copy.gossipMessageURI()); // Same messageURI
		assertEquals(original.messageAction(), ((GossipMessage) copy).messageAction());
	}
	
	@Test
	public void testCopyWithNewEmitterURIWithMessage() {
		GossipMessage original = new GossipMessage("broker-1");
		original.setRegisterMessage("reception-port-1", RegistrationClass.FREE);
		String newEmitterURI = "broker-2";
		
		GossipMessageI copy = original.copyWithNewEmitterURI(newEmitterURI);
		
		assertNotNull(((GossipMessage) copy).message());
		assertEquals(newEmitterURI, ((GossipMessage) copy).emitterURI);
		assertEquals(original.messageURI, copy.gossipMessageURI()); // Same messageURI
		assertEquals(original.messageAction(), ((GossipMessage) copy).messageAction());
	}
	
	@Test
	public void testCopyWithNewEmitterURIErrors() {
		GossipMessage original = new GossipMessage("broker-1");

		assertThrows(IllegalArgumentException.class, () -> original.copyWithNewEmitterURI(null));
		assertThrows(IllegalArgumentException.class, () -> original.copyWithNewEmitterURI(""));
	}
	
	// --------------------------------------------------------------------------
	// setPublishMessages tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetPublishMessages() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		ArrayList<MessageI> messages = new ArrayList<>();
		messages.add(new Message());
		
		gossipMessage.setPublishMessages("reception-port-1", "channel0", messages);
		
		assertEquals(Action.PUBLISH, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals("channel0", gossipMessage.message().getPropertyValue("channel"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
		assertNotNull(gossipMessage.message().getPayload()); // cannot test with Equals because the payload message reference has been changed while copying
	}
	
	@Test
	public void testSetPublishMessagesErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		ArrayList<MessageI> messages = new ArrayList<>();
		messages.add(new Message());
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setPublishMessages(null, "channel0", messages));
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setPublishMessages(null, "channel0", messages));
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setPublishMessages("port", "", messages));
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setPublishMessages("port", "channel0", null));
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setPublishMessages("port", "channel0", new ArrayList<>()));
	}
	
	// --------------------------------------------------------------------------
	// setRegisterMessage tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetRegisterMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		gossipMessage.setRegisterMessage("reception-port-1", RegistrationClass.FREE);
		
		assertEquals(Action.REGISTER_CLIENT, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals(RegistrationClass.FREE, gossipMessage.message().getPropertyValue("rc"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetRegisterMessageErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setRegisterMessage(null, RegistrationClass.FREE));
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setRegisterMessage("port", null));
	}
	
	// --------------------------------------------------------------------------
	// setUnregisterMessage tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetUnregisterMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		gossipMessage.setUnregisterMessage("reception-port-1");
		
		assertEquals(Action.UNREGISTER_CLIENT, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetUnregisterMessageErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setUnregisterMessage(null));
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setUnregisterMessage(""));
	}
	
	// --------------------------------------------------------------------------
	// setCreateChannelMessage tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetCreateChannelMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		gossipMessage.setCreateChannelMessage("reception-port-1", "channel0", ".*");
		
		assertEquals(Action.CREATE_CHANNEL, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals("channel0", gossipMessage.message().getPropertyValue("channel"));
			assertEquals(".*", gossipMessage.message().getPropertyValue("autorizedUsers"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetCreateChannelMessageErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setCreateChannelMessage("port", null, ".*"));
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setCreateChannelMessage("port", "channel0", null));
	}
	
	// --------------------------------------------------------------------------
	// setDestroyChannelMessage tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetDestroyChannelMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		gossipMessage.setDestroyChannelMessage("reception-port-1", "channel0", false);
		
		assertEquals(Action.DESTROY_CHANNEL, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals("channel0", gossipMessage.message().getPropertyValue("channel"));
			assertEquals(false, gossipMessage.message().getPropertyValue("now"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetDestroyChannelNowMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		gossipMessage.setDestroyChannelMessage("reception-port-1", "channel0", true);
		
		assertEquals(Action.DESTROY_CHANNEL, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals("channel0", gossipMessage.message().getPropertyValue("channel"));
			assertEquals(true, gossipMessage.message().getPropertyValue("now"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetDestroyChannelMessageErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setDestroyChannelMessage("port", "", false));
	}
	
	// --------------------------------------------------------------------------
	// setSubscribeClientMessage tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetSubscribeClientMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		MessageFilterI filter = new MessageFilter(new PropertyFilterI[0], new PropertiesFilterI[0], new JokerTimeFilter());
		
		gossipMessage.setSubscribeClientMessage("reception-port-1", "channel0", filter);
		
		assertEquals(Action.SUBSCRIBE_CLIENT_CHANNEL, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals("channel0", gossipMessage.message().getPropertyValue("channel"));
			assertEquals(filter, gossipMessage.message().getPropertyValue("filter"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetSubscribeClientMessageErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setSubscribeClientMessage("port", "channel", null));
	}
	
	// --------------------------------------------------------------------------
	// setUnsubscribeClientMessage tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetUnsubscribeClientMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		gossipMessage.setUnsubscribeClientMessage("reception-port-1", "channel0");
		
		assertEquals(Action.UNSUBSCRIBE_CLIENT_CHANNEL, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals("channel0", gossipMessage.message().getPropertyValue("channel"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetUnsubscribeClientMessageErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setUnsubscribeClientMessage("port", null));
	}
	
	// --------------------------------------------------------------------------
	// setUpdateClientClassMessage tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetUpdateClientClassMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		gossipMessage.setUpdateClientClassMessage("reception-port-1", RegistrationClass.PREMIUM);
		
		assertEquals(Action.UPDATE_CLIENT_SERVICE_CLASS, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals(RegistrationClass.PREMIUM, gossipMessage.message().getPropertyValue("rc"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetUpdateClientClassMessageErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setUpdateClientClassMessage("port", null));
	}
	
	// --------------------------------------------------------------------------
	// setUpdateChannelAuthURIsMessage tests
	// --------------------------------------------------------------------------
	
	@Test
	public void testSetUpdateChannelAuthURIsMessage() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		gossipMessage.setUpdateChannelAuthURIsMessage("reception-port-1", "channel0", ".*");
		
		assertEquals(Action.UPDATE_CHANNEL_AUTHORIZED_URIS, gossipMessage.messageAction());
		assertNotNull(gossipMessage.message());
		try {
			assertEquals("reception-port-1", gossipMessage.message().getPropertyValue("receptionPortURI"));
			assertEquals("channel0", gossipMessage.message().getPropertyValue("channel"));
			assertEquals(".*", gossipMessage.message().getPropertyValue("autorizedUsers"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSetUpdateChannelAuthURIsMessageErrors() {
		GossipMessage gossipMessage = new GossipMessage("broker-1");
		
		assertThrows(IllegalArgumentException.class, 
				() -> gossipMessage.setUpdateChannelAuthURIsMessage("port", "channel0", ""));
	}
}