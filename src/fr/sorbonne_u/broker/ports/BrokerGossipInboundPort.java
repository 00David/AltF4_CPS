package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.plugin.BrokerGossipPlugin;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipReceiverCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Inbound port of a {@link Broker}, allowing the handling of gossip messages reception.
 *
 *<p>Redirects incoming gossip messages to its {@link BrokerGossipPlugin}.</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerGossipInboundPort extends AbstractInboundPort implements GossipReceiverCI {

	private static final long serialVersionUID = -177954494080664983L;

	/**
	 * Creates the inbound port with the given {@code uri}, whose owner component {@code owner} is a {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * pre {@code owner instanceof Broker}
	 * pre {@code pluginURI != null && !pluginURI.isEmpty()}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    		the URI of the port.
	 * @param owner  		the component owning the port.
	 * @param pluginURI		the URI of the {@link BrokerGossipPlugin}.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public BrokerGossipInboundPort(String uri, Broker owner, String pluginURI) throws Exception {
		super(uri, GossipReceiverCI.class, owner, pluginURI, null);
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(pluginURI != null && !pluginURI.isEmpty()) : new PreconditionException("pluginURI cannot be null or empty");
	}
	
	// Precondition and postcondition verifications are delegated to the BrokerGossipPlugin
	
	@Override
	public void receive(GossipMessageI[] gossipMessages) throws Exception {
		BrokerGossipPlugin bgp = (BrokerGossipPlugin) this.getOwnerPlugin(this.pluginURI);
		this.getOwner().handleRequest(broker -> {
			bgp.receive(gossipMessages);
			return null;
		}); 
	}
}
