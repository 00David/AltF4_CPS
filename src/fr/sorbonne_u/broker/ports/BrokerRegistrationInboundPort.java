package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Inbound port of a {@link Broker}, allowing the handling of client registration.<br>
 * Delegates the management to its owner component, a {@link Broker}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerRegistrationInboundPort
extends		AbstractInboundPort
implements	RegistrationCI {

	private static final long serialVersionUID = 763740080899636307L;

	/**
	 * Creates the inbound port with the provided {@code uri}, whose owner component {@code owner} is a {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * pre {@code owner instanceof Broker}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    		the URI of the port.
	 * @param owner  		the component owning this port.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public BrokerRegistrationInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, RegistrationCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(owner instanceof Broker) : new PreconditionException("owner must be a Broker");
	}
	
	// Precondition and postcondition verifications are delegated to the Broker

	@Override
	public boolean registered(String receptionPortURI) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).registeredService(receptionPortURI)) ;
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception {
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
	public boolean channelExist(String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).channelExistService(channel));
	}
	
	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).channelAuthorisedService(receptionPortURI, channel));
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
