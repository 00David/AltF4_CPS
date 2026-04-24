package fr.sorbonne_u.broker.connectors;

import fr.sorbonne_u.broker.ports.ReceivingOutboundPort;
import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * Connector between a {@link ReceivingOutboundPort} and a port implementing {@link ReceivingCI}, 
 * delegates message reception to this port.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ReceivingServiceConnector extends AbstractConnector implements	ReceivingCI {

	// Precondition and postconditions verifications are delegated to the the port implementing ReceivingCI
	
	@Override
	public void receive(String channel, MessageI message) throws Exception {
		((ReceivingCI)this.offering).receive(channel, message);
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		((ReceivingCI)this.offering).receive(channel, messages);
	}

}
