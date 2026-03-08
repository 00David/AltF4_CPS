package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.connectors.BrokerReceivingServiceConnector;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Outbound port of a {@link Broker}, allowing the sending of one or more messages to a client.<br>
 * Delegates the sending to its {@link BrokerReceivingServiceConnector}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerReceivingOutboundPort
	extends		AbstractOutboundPort
	implements	ReceivingCI{
	
	private static final long serialVersionUID = -4068018179730158731L;

	/**
	 * Creates the outbound port with the provided {@code uri}, whose owner component {@code owner} is a {@link Broker}.
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
	public BrokerReceivingOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ReceivingCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(owner instanceof Broker) : new PreconditionException("owner must be a Broker");
	}
	
	// Precondition and postcondition verifications are delegated to the BrokerReceivingServiceConnector

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		((ReceivingCI)this.getConnector()).receive(channel, message) ;
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		((ReceivingCI)this.getConnector()).receive(channel, messages) ;
	}
}
