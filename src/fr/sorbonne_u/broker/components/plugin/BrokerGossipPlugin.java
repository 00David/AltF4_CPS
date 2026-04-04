package fr.sorbonne_u.broker.components.plugin;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.ports.BrokerGossipPluginInboundPort;
import fr.sorbonne_u.broker.ports.BrokerGossipPluginOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.cps.gossip.interfaces.GossipImplementationI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;

/**
 * The {@link Broker} plugin having a {@link BrokerGossipPluginInboundPort} and a {@link BrokerGossipPluginOutboundPort}.
 * 
 * <p>Handles {@link GossipMessageI} reception and sending</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	// TODO
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class BrokerGossipPlugin extends AbstractPlugin implements GossipImplementationI {

	private static final long serialVersionUID = -3119414073600533953L;

	@Override
	public void receive(GossipMessageI[] gossipMessages) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(GossipMessageI[] fromSender) {
		// TODO Auto-generated method stub
		
	}

}
