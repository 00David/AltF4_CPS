package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.connectors.BrokerGossipServiceConnector;
import fr.sorbonne_u.broker.plugin.BrokerGossipPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Outbound port of a {@link BrokerGossipPlugin}, allowing the sending of multiple {@link GossipMessageI} to another {@link BrokerGossipPlugin}.<br>
 * Delegates the sending to its {@link BrokerGossipServiceConnector}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerGossipPluginOutboundPort	extends AbstractOutboundPort implements GossipSenderCI{

	private static final long serialVersionUID = 2605143148791104560L;

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
	public BrokerGossipPluginOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, GossipSenderCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(owner instanceof Broker) : new PreconditionException("owner must be a Broker");
	}
	
	// Precondition and postcondition verifications are delegated to the BrokerGossipServiceConnector
	
	@Override
	public void send(GossipMessageI[] gossipMessages) throws Exception {
		((GossipSenderCI)this.getConnector()).send(gossipMessages) ;
	}
}
