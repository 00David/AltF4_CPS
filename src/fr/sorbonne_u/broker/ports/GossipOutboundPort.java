package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.connectors.GossipServiceConnector;
import fr.sorbonne_u.broker.plugin.BrokerGossipPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Delegates the sending of {@link GossipMessageI} to its {@link GossipServiceConnector}.
 * 
 * <p>
 * Used as an outbound port of a {@link BrokerGossipPlugin}, 
 * allowing the sending of multiple gossip messages to another {@link BrokerGossipPlugin}.
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
public class GossipOutboundPort	extends AbstractOutboundPort implements GossipSenderCI {

	private static final long serialVersionUID = 2605143148791104560L;

	/**
	 * Creates the outbound port with the provided {@code uri}, redirecting to a {@link GossipServiceConnector}.
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
	public GossipOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, GossipSenderCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
	}
	
	// Precondition and postcondition verifications are delegated to the GossipServiceConnector
	
	@Override
	public void send(GossipMessageI[] gossipMessages) throws Exception {
		((GossipSenderCI)this.getConnector()).send(gossipMessages) ;
	}
}
