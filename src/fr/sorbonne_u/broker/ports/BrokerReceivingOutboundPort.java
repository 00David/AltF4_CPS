package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * Port sortant d'un Broker, permettant d'envoyer un ou plusieurs messages à un client.<br>
 * Délègue l'envoie à son {@link ConnectorI}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerReceivingOutboundPort
	extends		AbstractOutboundPort
	implements	ReceivingCI{
	
	private static final long serialVersionUID = -4068018179730158731L;

	/**
	 * Crée le port sortant avec l'{@code uri} fournie, dont le composant posseseur {@code owner} est un {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	uri != null and owner != null
	 * pre	owner instanceof {@link Broker}
	 * post	true			// pas de postcondition.
	 * </pre>
	 * 
	 * @param uri          l'URI du port.
	 * @param owner        Le composant possesseur du port.
	 */
	public BrokerReceivingOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ReceivingCI.class, owner) ;
		// Vérification des préconditions
		assert(uri != null);
		assert(owner != null);
		assert(owner instanceof Broker);
	}

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		// Vérification des préconditions
		assert(channel != null);
		assert(!channel.isEmpty());
		
		((ReceivingCI)this.getConnector()).receive(channel, message) ;
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		// Vérification des préconditions
		assert(channel != null);
		assert(!channel.isEmpty());
		
		((ReceivingCI)this.getConnector()).receive(channel, messages) ;
	}
}
