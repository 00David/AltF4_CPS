package fr.sorbonne_u.messages;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Classe représentant un couple : nom de propriété / valeur de la propriété.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class Property implements PropertyI{

	private static final long serialVersionUID = -5677422397857856072L;
	private String name;
	private Serializable value;

	/**
	 * Construit un couple : nom de propriété / valeur de la propriété.
	 * @param name Nom de la propriété.
	 * @param value Valeur de la propriété.
	 * @throws IllegalArgumentException Si un des arumgents est {@code null}.
	 */
	public Property(String name, Serializable value) {
		if (name == null) throw new IllegalArgumentException("A property name cannot be null");
		if (value == null) throw new IllegalArgumentException("A property value cannot be null");
		this.name = name;
		this.value = value;
	}

	@Override
	public String getName() {
		// Postconditions vérifiées par le constructeur
		return name;
	}

	@Override
	public Serializable getValue() {
		return value;
	}

}
