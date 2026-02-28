package fr.sorbonne_u.clients.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * Inbound port of a {@link Broker}, allowing the handling of messages reception.<br>
 * Delegates the processing to its owner component, a {@link Client}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientReceivingInboundPort extends AbstractInboundPort implements ReceivingCI {

	private static final long serialVersionUID = 3472768325829807000L;

	/**
	 * Creates the inbound port with the given {@code uri}, whose owner component {@code owner} is a {@link Client}.
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
	public ClientReceivingInboundPort(String uri,ComponentI owner) throws Exception {
		super(uri,ReceivingCI.class,owner);
		// Precondition checks
		assert(uri != null && !uri.isEmpty());
		assert(owner != null);
		assert(owner instanceof Client);
	}
	
	// Precondition and postcondition verifications are delegated to the Client

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Client)owner).receiveService(channel, message));
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Client)owner).receiveService(channel, messages));
	}
}