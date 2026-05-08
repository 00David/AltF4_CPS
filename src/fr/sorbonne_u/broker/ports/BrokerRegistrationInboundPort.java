package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Inbound port of a {@link Broker}, allowing the handling of client registration.
 * 
 * <p>Delegates the management to its owner component, a {@link Broker}.</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerRegistrationInboundPort extends AbstractInboundPort implements RegistrationCI {

	private static final long serialVersionUID = 763740080899636307L;

	/**
	 * Creates the inbound port with the provided {@code uri}, whose owner component {@code owner} is a {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    		the URI of the port.
	 * @param owner  		the component owning this port.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public BrokerRegistrationInboundPort(String uri, Broker owner) throws Exception {
		super(uri, RegistrationCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
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
				owner -> ((Broker)owner).registerService(receptionPortURI, rc, true));
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyServiceClassService(receptionPortURI, rc, true));
	}

	@Override
	public void unregister(String receptionPortURI) throws Exception {
		this.getOwner().handleRequest(
				owner -> {
					((Broker)owner).unregisterService(receptionPortURI, true);
					return null;
				});
	}

	@Override
	public boolean channelExist(String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).channelExistService(channel));
	}
	
	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> {
					((Broker)owner).channelAuthorisedService(receptionPortURI, channel);
					return null;
				});
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).subscribedService(receptionPortURI, channel));
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		this.getOwner().handleRequest(
				owner -> {
					((Broker)owner).subscribeService(receptionPortURI, channel, filter, true);
					return null;
				});
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> {
					((Broker)owner).unsubscribeService(receptionPortURI, channel, true);
					return null;
				});
	}

	@Override
	public void modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		this.getOwner().handleRequest(
				owner -> {
					((Broker)owner).modifyFilterService(receptionPortURI, channel, filter);
					return null;
				});
	}

	
}
