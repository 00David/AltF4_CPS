package fr.sorbonne_u.messages.tests;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

public class LocationProperty implements PropertyI {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String name;
	private Serializable value;
	
	public LocationProperty(Serializable value) {
		this.name = "location";
		this.value = value;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Serializable getValue() {
		return this.value;
	}

}
