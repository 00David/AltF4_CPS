package fr.sorbonne_u.messages.tests;

import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;
import fr.sorbonne_u.messages.Message;
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

	@Test
	public void testRemoveNotExistingProperty() {
		Message message = new Message();
		assertThrows(UnknownPropertyException.class, () -> message.removeProperty("place"));
	}
	
	@Test
	public void testRemoveProperty() {
		Message message = new Message();
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
		Message message = new Message();
		assertThrows(UnknownPropertyException.class, ()-> message.getPropertyValue("place"));
	}
	
	
	
	@Test
	public void testGetPropertyValue() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		PropertyI value = null;
		try {
			value = (PropertyI) message.getPropertyValue("location");
		} catch (UnknownPropertyException e) {
			e.printStackTrace();
		}
		
		assertTrue(value instanceof PropertyI);
	}
	
	@Test
	public void testPropertyNotExists() {
		Message message = new Message();
		assertFalse(()-> message.propertyExists("place"));
	}
	
	
	@Test
	public void testPropertyExists() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		
		assertTrue(message.propertyExists("location"));
	}
	
	
	@Test
	public void testPutProperty() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		
		assertTrue(message.propertyExists("location"));
	}
	
	
	
	@Test
	public void testGetProperties() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		message.putProperty("location", data);
		
		PropertyI[] properties = message.getProperties();
		
		assertNotNull(properties);
		
	}
	
	@Test
	public void testGetTimeStamp() {
		Instant before = Instant.now();
        Message message = new Message();
        Instant after = Instant.now();
        Instant ts = message.getTimeStamp();

        assertNotNull(ts);
        assertFalse(ts.isBefore(before));
        assertFalse(ts.isAfter(after));
		
	}
	
	@Test
	public void testSetPayload() {
		Message message = new Message();
		WindData exemple = new WindData(new PositionData(0, 0), 0, 0);
		message.setPayload(exemple);
		
		assertTrue(message.getPayload() instanceof Serializable);
		
	}
	
	@Test
	public void testGetPayload() {
		Message message = new Message();
		WindData exemple = new WindData(new PositionData(0, 0), 0, 0);
		message.setPayload(exemple);
		
		assertNotNull(message.getPayload());
		assertTrue(message.getPayload() instanceof Serializable);
	}
	
	@Test
	public void testCopy() {
		Message message = new Message();
		WindData exemple = new WindData(new PositionData(0, 0), 0, 0);
		message.setPayload(exemple);
		
		Message copy = (Message) message.copy();
		
		assertEquals(copy.getPayload(), message.getPayload());
	}
}
