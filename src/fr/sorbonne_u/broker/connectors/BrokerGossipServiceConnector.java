package fr.sorbonne_u.broker.connectors;

import fr.sorbonne_u.broker.plugin.BrokerGossipPlugin;
import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;

/**
 * Connector between an outbound port of the {@link BrokerGossipPlugin} component and an inbound port of another {@link BrokerGossipPlugin}.<br>
 * Delegates gossip message reception to the {@link BrokerGossipPlugin} receiving.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerGossipServiceConnector extends AbstractConnector implements GossipSenderCI{

	// Precondition and postconditions verifications are delegated to the the BrokerGossipPluginInboundPort
	
	@Override
	public void send(GossipMessageI[] gossipMessages) throws Exception {
		((GossipSenderCI)this.offering).send(gossipMessages);
	}

}
