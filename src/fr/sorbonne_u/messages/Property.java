package fr.sorbonne_u.messages;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

// J'en avais besoin pour mes tests sur les filtres de propriétés ...
public class Property implements PropertyI{

	private static final long serialVersionUID = -5677422397857856072L;
	private String name;
	private Serializable value;

	public Property(String name, Serializable value) {
		this.name = name;
		this.value = value;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Serializable getValue() {
		return value;
	}

}
