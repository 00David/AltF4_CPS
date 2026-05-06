package fr.sorbonne_u.messages;

import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.time.Instant;

/**
 * Unit tests for {@link Message}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class MessageTest{
	
	/**
	 * Utility class for tests.
	 * 
	 * <p>Used by {@link MessageTest} for having a pair of strings : longitude / latitude.</p>
	 * 
	 * <p><strong>Invariants</strong></p>
	 * <pre>
	 * invariant	{@code true} // no more invariant
	 * </pre>
	 * @author Kevin Tang
	 * @author David Vadimon
	 */
	private static class LocationData implements Serializable {
		private static final long serialVersionUID = 6442774187550548688L;
		@SuppressWarnings("unused")
		private String longitude;
		@SuppressWarnings("unused")
		private String latitude;
		
		/**
		 * Creates a pair of strings : longitude / latitude
		 * @param longitude The longitude of the pair.
		 * @param latitude	The latitude of the pair.
		 */
		public LocationData(String longitude,String latitude) {
			this.longitude = longitude;
			this.latitude = latitude;
		}
	}
	
	/**
	 * Utility class for tests.
	 * 
	 * <p>Used by various test classes across the project for having a concrete {@link Message}.</p>
	 * 
	 * <p><strong>Invariants</strong></p>
	 * <pre>
	 * invariant	{@code true} // no more invariant
	 * </pre>
	 * @author Kevin Tang
	 * @author David Vadimon
	 */
	public static class DummyMessage extends Message {
		private static final long serialVersionUID = -984644676868145164L;

		@Override
		public MessageI copy() {
			DummyMessage copy = new DummyMessage();
			copy.setCopiedContent(this);
			return copy;
		}
	}

	@Test
	public void testRemoveNotExistingProperty() {
		Message message = new DummyMessage();
		assertThrows(UnknownPropertyException.class, () -> message.removeProperty("place"));
	}
	
	@Test
	public void testRemoveProperty() {
		Message message = new DummyMessage();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		
		try {
			message.removeProperty("location");
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
		
		assertFalse(message.propertyExists("location"));
		
		
	}
	
	@Test
	public void testGetValueNotExistingProperty() {
		Message message = new DummyMessage();
		assertThrows(UnknownPropertyException.class, ()-> message.getPropertyValue("place"));
	}
	
	
	
	@Test
	public void testGetPropertyValue() {
		Message message = new DummyMessage();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		try {
			assertEquals(data, message.getPropertyValue("location"));
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testPropertyNotExists() {
		Message message = new DummyMessage();
		assertFalse(()-> message.propertyExists("place"));
	}
	
	
	@Test
	public void testPropertyExists() {
		Message message = new DummyMessage();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		
		assertTrue(message.propertyExists("location"));
	}
	
	
	@Test
	public void testPutProperty() {
		Message message = new DummyMessage();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		
		assertTrue(message.propertyExists("location"));
	}
	
	
	
	@Test
	public void testGetProperties() {
		Message message = new DummyMessage();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		
		PropertyI[] properties = message.getProperties();
		
		assertNotNull(properties);
		
	}
	
	@Test
	public void testGetTimeStamp() {
		Instant before = Instant.now();
        Message message = new DummyMessage();
        Instant after = Instant.now();
        Instant ts = message.getTimeStamp();

        assertNotNull(ts);
        assertFalse(ts.isBefore(before));
        assertFalse(ts.isAfter(after));
		
	}
	
	@Test
	public void testSetPayload() {
		Message message = new DummyMessage();
		WindData exemple = new WindData(new PositionData(0, 0), 0, 0);
		message.setPayload(exemple);
		
		assertTrue(message.getPayload() instanceof Serializable);
		
	}
	
	@Test
	public void testGetPayload() {
		Message message = new DummyMessage();
		WindData exemple = new WindData(new PositionData(0, 0), 0, 0);
		message.setPayload(exemple);
		
		assertNotNull(message.getPayload());
		assertTrue(message.getPayload() instanceof Serializable);
	}
	
	@Test
	public void testCopy() {
		Message message = new DummyMessage();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		message.setPayload(data);
		
		Message copy = (Message) message.copy();
		PropertyI[] properties = copy.getProperties();
		
		assertEquals(message.getPayload(), copy.getPayload());
		assertEquals(1, properties.length);
		assertTrue(copy.propertyExists("location"));
		PropertyI p = properties[0];
		assertEquals(data, p.getValue());
	}
}
