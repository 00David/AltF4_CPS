package fr.sorbonne_u.messages;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;

import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * An abstract message class for the publication/subscription system.
 * 
 * <p>{@link MessageI#copy()} method needs to be implemented in a concrete sub-class.</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code timeStamp != null}
 * invariant	{@code properties != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public abstract class Message implements MessageI {

	private static final long serialVersionUID = -6675042347367398334L;
	
	/** The payload of the message */
	protected Serializable payload;
	
	/** The timeStamp of the message, initialized with the message creation time (not while setting the payload) */
	protected Instant timeStamp;
	
	/** A mapping : name of a message's property -> the corresponding property object */
	protected HashMap<String, PropertyI> properties;
	
	/**
	 * Creates an empty message.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	class invariants satisfied
	 * </pre>
	 */
	public Message() { 
		this.payload = null;
		this.properties = new HashMap<String,PropertyI>();
		this.timeStamp = Instant.now();
	}
	
	/**
	 * @throws IllegalArgumentException		if {@code name} is null or empty.
	 * @see MessageI#propertyExists(String)
	 */
	@Override
	public boolean propertyExists(String name) {
		// Precondition checks
		if (name == null || name.isEmpty()) 
			throw new IllegalArgumentException("A name cannot be null or empty");
		
		return this.properties.containsKey(name);
	}
	
	/**
	 * @throws IllegalArgumentException		if {@code name} is null, is empty, or a property already has that name.
	 * @see MessageI#putProperty(String, Serializable)
	 */
	@Override
	public void putProperty(String name, Serializable value) {
		// Precondition checks
		if (name == null || name.isEmpty()) 
			throw new IllegalArgumentException("A name cannot be null or empty");
		if (propertyExists(name)) 
			throw new IllegalArgumentException("A property already has that name");
		
		this.properties.put(name, new Property(name, value));
		assert(propertyExists(name)); // Postcondition check
	}
	
	/**
	 * @throws IllegalArgumentException		if {@code name} is null or empty.
	 * @see MessageI#removeProperty(String)
	 */
	@Override
	public void removeProperty(String name) throws UnknownPropertyException {
		// Precondition checks
		if (name == null || name.isEmpty()) 
			throw new IllegalArgumentException("A name cannot be null or empty");
		
		if(!propertyExists(name)) 
			throw new UnknownPropertyException("Cannot remove '" + name + "' property because it does not exists");
		
		this.properties.remove(name);
		assert(!propertyExists(name)); // Postcondition check
	}
	
	/**
	 * @throws IllegalArgumentException		if {@code name} is null or empty.
	 * @see MessageI#getPropertyValue(String)
	 */
	@Override
	public Serializable getPropertyValue(String name) throws UnknownPropertyException {
		// Precondition checks
		if (name == null || name.isEmpty()) 
			throw new IllegalArgumentException("A name cannot be null or empty");
		
		if (propertyExists(name)) {
			return this.properties.get(name).getValue();
		} else {
			throw new UnknownPropertyException("Cannot get value of '" + name + "' property because it does not exists");
		}
	}
	
	@Override
	public PropertyI[] getProperties() {
		// .toArray(new PropertyI[0]) : converts the Collection to a PropertyI array
		PropertyI[] props = this.properties.values().toArray(new PropertyI[0]);
		assert (props != null); // Postcondition check
		return props;
	}
	
	@Override
	public abstract MessageI copy(); // Put here for an informational purpose

	/**
	 * Utility method for a concrete implementation of {@link MessageI#copy()}
	 * into a sub-class.
	 * 
	 * <p>
	 * Copy every property and its associated value from a given {@code originalMessage}
	 * into the current message.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param originalMessage The message from which we want to copy properties into the current message.
	 */
	protected void setCopiedContent(Message originalMessage) {
		for(PropertyI p : originalMessage.properties.values()) {
			this.putProperty(p.getName(), p.getValue());
		}
		this.setPayload(originalMessage.payload);
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
		// Postcondition satisfied by class invariant
		return this.timeStamp;
	}
	
	@Override
	public String toString() {
		if (this.payload != null) {
			return this.payload.toString();
		} else {
			return super.toString();
		}
	}

}
