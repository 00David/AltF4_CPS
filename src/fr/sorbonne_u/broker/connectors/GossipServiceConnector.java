package fr.sorbonne_u.broker.connectors;

import fr.sorbonne_u.broker.ports.GossipOutboundPort;
import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipReceiverCI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;

/**
 * Connector between a {@link GossipOutboundPort} and a port implementing {@link GossipSenderCI}, 
 * delegates gossip message reception to this port.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class GossipServiceConnector extends AbstractConnector implements GossipSenderCI{

	// Precondition and postconditions verifications are delegated to the the port implementing GossipSenderCI

	@Override
	public void send(GossipMessageI[] gossipMessages) throws Exception {
		((GossipReceiverCI)this.offering).receive(gossipMessages);
	}

}
