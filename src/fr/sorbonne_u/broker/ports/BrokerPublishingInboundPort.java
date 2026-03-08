package fr.sorbonne_u.broker.ports;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Inbound port of a {@link Broker}, allowing the handling of client registration.<br>
 * Delegates the processing to its owner component, a {@link Broker}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerPublishingInboundPort
extends		AbstractInboundPort
implements	PublishingCI {

	private static final long serialVersionUID = 1878031196993861332L;

	/**
	 * Creates the inbound port with the given {@code uri}, whose owner component {@code owner} is a {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * pre {@code owner instanceof Broker}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    the URI of the port.
	 * @param owner  the component owning the port.
	 */
	public BrokerPublishingInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, PublishingCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(owner instanceof Broker) : new PreconditionException("owner must be a Broker");
	}
	
	// Used for BrokerPrivilegedInboundPort constructor
	public BrokerPublishingInboundPort(String uri, Class<? extends OfferedCI> implementedInterface, ComponentI owner) throws Exception {
		super(uri, implementedInterface, owner);
	}

	// Precondition and postcondition verifications are delegated to the Broker

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		ArrayList<MessageI> messages =  new ArrayList<MessageI>();
		messages.add(message);
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).publishService(receptionPortURI, channel, messages));
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).publishService(receptionPortURI, channel, messages));
	}
	
}
