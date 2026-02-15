package fr.sorbonne_u.messages.tests;



import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;
import fr.sorbonne_u.messages.Message;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.time.Instant;


public class MethodsMessageTest{
	
	
	
	@Test
	public void testRemoveNotExistingProperty() {
		Message message = new Message();
		assertThrows(UnknownPropertyException.class, () -> message.removeProperty("lieu"));
	}
	
	
	
	@Test
	public void testRemoveProperty() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		LocationProperty ex = new LocationProperty(data);
		message.putProperty("location", ex);
		
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
		assertThrows(UnknownPropertyException.class, ()-> message.getPropertyValue("lieu"));
	}
	
	
	
	@Test
	public void testGetPropertyValue() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		LocationProperty ex = new LocationProperty(data);
		message.putProperty("location", ex);
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
		assertFalse(()-> message.propertyExists("lieu"));
	}
	
	
	@Test
	public void testPropertyExists() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		LocationProperty ex = new LocationProperty(data);
		message.putProperty("location", ex);
		
		assertTrue(message.propertyExists("location"));
	}
	
	
	@Test
	public void testPutProperty() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		LocationProperty ex = new LocationProperty(data);
		message.putProperty("location", ex);
		
		assertTrue(message.propertyExists("location"));
	}
	
	
	
	@Test
	public void testGetProperties() {
		Message message = new Message();
		LocationData data = new LocationData("10248773", "6437392729");
		LocationProperty ex = new LocationProperty(data);
		message.putProperty("location", ex);
		
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
		WindData exemple = new WindData();
		message.setPayload(exemple);
		
		assertTrue(message.getPayload() instanceof Serializable);
		
	}
	
	@Test
	public void testGetPayload() {
		Message message = new Message();
		WindData exemple = new WindData();
		message.setPayload(exemple);
		
		assertNotNull(message.getPayload());
		assertTrue(message.getPayload() instanceof Serializable);
	}
	
	public void testCopy() {
		Message message = new Message();
		WindData exemple = new WindData();
		message.setPayload(exemple);
		
		Message copy = (Message) message.copy();
		
		assertEquals(copy.getPayload(), message.getPayload());
	}

}
