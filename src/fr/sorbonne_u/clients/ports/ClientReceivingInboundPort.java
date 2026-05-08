package fr.sorbonne_u.clients.ports;

import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Inbound port of a {@link Client}, allowing the handling of messages reception.

 *<p>Redirects incoming messages to its {@link ClientSubscriptionPlugin}.</p>
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
	 * pre {@code pluginURI != null && !pluginURI.isEmpty()}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    		the URI of the port.
	 * @param owner  		the component owning the port.
	 * @param pluginURI		the URI of the {@link ClientSubscriptionPlugin}.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public ClientReceivingInboundPort(String uri, Client owner, String pluginURI) throws Exception {
		super(uri, ReceivingCI.class, owner, pluginURI, null);
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(pluginURI != null && !pluginURI.isEmpty()) : new PreconditionException("pluginURI cannot be null or empty");
	}
	
	// Precondition and postcondition verifications are delegated to the ClientSubscriptionPlugin

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		ClientSubscriptionPlugin csp = (ClientSubscriptionPlugin) this.getOwnerPlugin(this.pluginURI);
		this.getOwner().runTask(client -> csp.handleIncomingReception(channel, new MessageI[] {message}));
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		ClientSubscriptionPlugin csp = (ClientSubscriptionPlugin) this.getOwnerPlugin(this.pluginURI);
		this.getOwner().runTask(client -> csp.handleIncomingReception(channel, messages));
	}
}