package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.broker.connectors.ReceivingServiceConnector;
import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Delegates the sending of {@link MessageI} to its {@link ReceivingServiceConnector}.
 * 
 * <p>
 * Used as an outbound port of a {@link Broker}, 
 * allowing the sending of one or more messages to a {@link Client}.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ReceivingOutboundPort extends AbstractOutboundPort implements ReceivingCI{
	
	private static final long serialVersionUID = -4068018179730158731L;

	/**
	 * Creates the outbound port with the provided {@code uri} redirecting to a {@link ReceivingServiceConnector}.
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
	public ReceivingOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ReceivingCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
	}
	
	// Precondition and postcondition verifications are delegated to the ReceivingServiceConnector

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		((ReceivingCI)this.getConnector()).receive(channel, message) ;
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		((ReceivingCI)this.getConnector()).receive(channel, messages) ;
	}
}
