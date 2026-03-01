package fr.sorbonne_u.clients.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * Inbound port of a {@link Broker}, allowing the handling of messages reception.<br>
 * Delegates the processing to the {@link ClientSubscriptionPlugin} of its component, a {@link Client}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientReceivingPluginInboundPort extends AbstractInboundPort implements ReceivingCI {

	private static final long serialVersionUID = 3472768325829807000L;

	/**
	 * Creates the inbound port with the given {@code uri}, whose owner component {@code owner} is a {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * pre {@code owner instanceof Client}
	 * pre {@code pluginURI != null && !pluginURI.isEmpty()}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    	the URI of the port.
	 * @param owner  	the component owning the port.
	 * @param pluginURI	the URI of the {@link ClientSubscriptionPlugin}.
	 */
	public ClientReceivingPluginInboundPort(String uri, ComponentI owner, String pluginURI) throws Exception {
		super(uri, ReceivingCI.class, owner, pluginURI, null);
		// Precondition checks
		assert(uri != null && !uri.isEmpty());
		assert(owner != null);
		assert(owner instanceof Client);
		assert(pluginURI != null && !pluginURI.isEmpty());
	}
	
	// Precondition and postcondition verifications are delegated to the Client

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		this.getOwner().handleRequest(this.getPluginURI(), 
				owner -> ((Client)owner).receiveService(channel, message));
		this.getOwner().handleRequest(
				new AbstractComponent.AbstractService<Void>(this.getPluginURI()) {
					@Override
					public Void call() throws Exception {
						((ClientSubscriptionPlugin)
								this.getServiceProviderReference()).receive(channel, message);
						return null;
					}
				});
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		this.getOwner().handleRequest(
				new AbstractComponent.AbstractService<Void>(this.getPluginURI()) {
					@Override
					public Void call() throws Exception {
						((ClientSubscriptionPlugin)
								this.getServiceProviderReference()).receive(channel, messages);
						return null;
					}
				});
	}
}