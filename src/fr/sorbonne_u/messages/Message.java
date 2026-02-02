package fr.sorbonne_u.messages;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

public class Message implements MessageI {
	
	
	private static final long serialVersionUID = 1L;
	protected Serializable payload;
	protected Instant timeStamp;
	protected HashMap<String, PropertyI> properties;
	
	public Message() { 
		this.properties = new HashMap<String,PropertyI>();
		this.timeStamp = Instant.now();
	}
	
	@Override
	public boolean propertyExists(String name) {
		
		return this.properties.containsKey(name);
	}

	@Override
	public void putProperty(String name, Serializable value) {
		this.properties.put(name, new Property(name, value));

	}

	@Override
	public void removeProperty(String name) throws UnknownPropertyException {
		
		if(!propertyExists(name)) {
			String msg = "Cannot remove " + name + " property because it does not exists";
			throw new UnknownPropertyException(msg);
		}
		
		this.properties.remove(name);

	}

	@Override
	public Serializable getPropertyValue(String name) throws UnknownPropertyException {
		
		if(propertyExists(name)) {
			return this.properties.get(name);
		}else {
			String msg = "Cannot get value of " + name + " property because it does not exists"; 
			throw new UnknownPropertyException(msg);
		}
	}

	@Override
	public PropertyI[] getProperties() {
		return this.properties.values().toArray(new PropertyI[0]);
	}

	@Override
	public MessageI copy() {
		Message copiedMessage = new Message();
		for(PropertyI p : this.properties.values()) {
			copiedMessage.putProperty(p.getName(), p);
		}
		copiedMessage.setPayload(this.payload);
		copiedMessage.timeStamp = this.timeStamp;
		
		return copiedMessage;
	}

	@Override
	public void setPayload(Serializable payload) {
		this.payload = payload;
	}

	@Override
	public Serializable getPayload() {
		return this.payload;
	}

	@Override
	public Instant getTimeStamp() {
		return this.timeStamp;
	}

}
