package fr.sorbonne_u.messages;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;

import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;


/**
 * La classe <code>Message</code> implemente un message dans le systeme publication/souscription.
 */
public class Message implements MessageI {
	
	
	private static final long serialVersionUID = 1L;
	protected Serializable payload;
	protected Instant timeStamp;
	protected HashMap<String, PropertyI> properties;
	
	
	/**
	 * Constructeur de la classe <code>Message</code>
	 */
	public Message() { 
		this.properties = new HashMap<String,PropertyI>();
		this.timeStamp = Instant.now();
	}
	
	
	/**
	 * 	Retourne si la propriete existe dans le message
	 * @param name Nom de la propriété
	 * @return <code>true</code> si la propriété existe sinon <code>false</code>
	 * @return 
	 */
	@Override
	public boolean propertyExists(String name) {
		
		return this.properties.containsKey(name);
	}

	
	/**
	 * Ajoute la propriété dans le message.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * 
	 * <pre>
	 * pre	{@code name != null && !name.isEmpty()}
	 * pre	{@code !propertyExists(name)}
	 * post	{@code propertyExists(name)}
	 * </pre>
	 * 
	 * 
	 * @param name Nom de la propriété à ajouter.
	 * @param value Valeur de la propriété à ajouter.
	 */
	@Override
	public void putProperty(String name, Serializable value) {
		
		this.properties.put(name, new Property(name, value));

	}

	
	/**
	 * Enleve la propriété {@code name} du message.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * 
	 * <pre>
	 * pre	{@code name != null && !name.isEmpty()}
	 * post	{@code !propertyExists(name)}
	 * <pre>
	 * 
	 * @param name Nom de la propriété à retirer.
	 * @throws UnknownPropertyException quand il n'existe pas de propriété {@code name} dans le message
	 * 
	 * 
	 */
	@Override
	public void removeProperty(String name) throws UnknownPropertyException {
		
		if(!propertyExists(name)) {
			String msg = "Cannot remove " + name + " property because it does not exists";
			throw new UnknownPropertyException(msg);
		}
		
		this.properties.remove(name);

	}

	
	/**
	 * Retourne la valeur de la propriété {@code name} du message.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre	{@code name != null && !name.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param name Nom de la propriété à recuperer.
	 * @return  la valeur associé à {@code name} du message.
	 * @throws UnknownPropertyException quand il n'existe pas de propriete {@code name} dans le message
	 * 
	 */
	@Override
	public Serializable getPropertyValue(String name) throws UnknownPropertyException {
		
		if(propertyExists(name)) {
			return this.properties.get(name);
		}else {
			String msg = "Cannot get value of " + name + " property because it does not exists"; 
			throw new UnknownPropertyException(msg);
		}
	}

	
	/**
	 * Retourne un tableau de propriété defini dans le message
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * 
	 * @return le tableau de propriété defini dans le message
	 * 
	 * 
	 * 
	 */
	@Override
	public PropertyI[] getProperties() {
		return this.properties.values().toArray(new PropertyI[0]);
	}

	
	/**
	 * Copie le message en faisant une copie profonde des propriete et le payload n'est pas copie 
	 * mais simplement referencé par le nouveau message.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * @return une copie du message avec les memes propriétés et payload.
	 * 
	 */
	@Override
	public MessageI copy() {
		Message copiedMessage = new Message();
		for(PropertyI p : this.properties.values()) {
			copiedMessage.putProperty(p.getName(), p);
		}
		copiedMessage.setPayload(this.payload);
		
		
		return copiedMessage;
	}

	/**
	 * Met à jour le payload avec le nouveau <code>payload</code>
	 */
	@Override
	public void setPayload(Serializable payload) {
		this.payload = payload;
	}

	
	/**
	 * Retourne le payload du message
	 */
	@Override
	public Serializable getPayload() {
		return this.payload;
	}

	
	
	/**
	 * Retourne le timestamp du message
	 */
	@Override
	public Instant getTimeStamp() {
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
