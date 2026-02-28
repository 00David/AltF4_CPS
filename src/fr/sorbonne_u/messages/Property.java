package fr.sorbonne_u.messages;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Class implementing a couple : property name / property value.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code name != null && !name.isEmpty()}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class Property implements PropertyI{

	private static final long serialVersionUID = -5677422397857856072L;
	private String name;
	private Serializable value;

	/**
	 * Creates a couple : property name / property value.
	 * 
	 * <pre>
	 * pre	{@code name != null && !name.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param name 							name of the property.
	 * @param value 						value of the property.
	 * @throws IllegalArgumentException		if a precondition is not satisfied.
	 */
	public Property(String name, Serializable value) {
		// Precondition checks
		if (name == null || name.isEmpty()) throw new IllegalArgumentException("A property name cannot be null or empty");
		
		this.name = name;
		this.value = value;
	}

	@Override
	public String getName() {
		return name;
		// Postcondition satisfied by class invariant
	}
	
	@Override
	public Serializable getValue() {
		return value;
	}
}
