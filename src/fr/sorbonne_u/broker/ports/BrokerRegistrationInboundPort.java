package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * Port entrant d'un Broker, permettant de gérer l'enregistrement d'un client.<br>
 * Délègue la gestion à son possesseur, un composant {@link Broker}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerRegistrationInboundPort
extends		AbstractInboundPort
implements	RegistrationCI {

	private static final long serialVersionUID = 763740080899636307L;

	/**
	 * Crée le port entrant avec l'{@code uri} fournie, dont le composant posseseur {@code owner} est un {@link Broker}.
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
	public BrokerRegistrationInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ReceivingCI.class, owner) ;
		// Vérification des préconditions
		assert(uri != null);
		assert(owner != null);
		assert(owner instanceof Broker);
	}
	
	// Quasiment toutes les préconditions et postconditions sont vérifiées par délégation, lors de l'exécution du service du Broker appellé.
	// Si ce n'est pas le cas, directement vérifiées ici.
	
	@Override
	public boolean registered(String receptionPortURI) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).registeredService(receptionPortURI, null)) ;
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception {
		assert(rc != null);
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).registeredService(receptionPortURI, rc)) ;
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).registerService(receptionPortURI, rc));
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyServiceClassService(receptionPortURI, rc));
	}

	@Override
	public void unregister(String receptionPortURI) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).unregisterService(receptionPortURI));
	}

	@Override
	public boolean channelExists(String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).channelExistsService(channel));
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).subscribedService(receptionPortURI, channel));
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).subscribeService(receptionPortURI, channel, filter));
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).unsubscribeService(receptionPortURI, channel));
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyFilterService(receptionPortURI, channel, filter));
	}
	
}
