package fr.sorbonne_u.messages.tests;



import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
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
	
	//à faire removeProperty existant
	
	@Test
	public void testGetValueNotExistingProperty() {
		Message message = new Message();
		assertThrows(UnknownPropertyException.class, ()-> message.getPropertyValue("lieu"));
	}
	
	//à faire getPropertyValue property existant
	
	@Test
	public void testPropertyNotExists() {
		Message message = new Message();
		assertFalse(()-> message.propertyExists("lieu"));
	}
	
	//à faire PropertyExists property existant
	
	//à faire PutProperty
	
	
	//à faire getProperties
	
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
	}
	
	public void testCopy() {
		Message message = new Message();
		WindData exemple = new WindData();
		message.setPayload(exemple);
		
		Message copy = (Message) message.copy();
		
		assertEquals(copy.getPayload(), message.getPayload());
	}

}
