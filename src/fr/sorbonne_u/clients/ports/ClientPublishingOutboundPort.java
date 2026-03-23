package fr.sorbonne_u.clients.ports;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;

/**
 * Outbound port of a {@link Client}, allowing the publication of one or more messages to a {@link Broker}.
 * Delegates the publication to its {@link ClientPublishingServiceConnector}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientPublishingOutboundPort extends AbstractOutboundPort implements PublishingCI{

	private static final long serialVersionUID = 2672780904734490388L;

	/**
	 * Creates the outbound port with the provided {@code uri}, whose owner component {@code owner} is a {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * pre {@code owner instanceof Client}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    		the URI of the port.
	 * @param owner  		the component owning this port.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public ClientPublishingOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, PublishingCI.class, owner);
		// Precondition checks
		assert(uri != null && !uri.isEmpty());
		assert(owner != null);
		assert(owner instanceof Client);
	}
	
	/**
	 * Creates the outbound port with the provided {@code uri}, whose owner component {@code owner} is a {@link Client}.<br>
	 * Used by {@link ClientPrivilegedOutboundPort} constructor.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * pre {@code owner instanceof Client}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    					the URI of the port.
	 * @param implementedInterface    	the implemented interface, extending {@link RequiredCI}.
	 * @param owner  					the component owning this port.
	 * @throws Exception 				if an error occurs while creating the port
	 */
	public ClientPublishingOutboundPort(String uri, Class<? extends RequiredCI> implementedInterface, ComponentI owner) throws Exception {
		super(uri, implementedInterface, owner);
		// Precondition checks
		assert(uri != null && !uri.isEmpty());
		assert(owner != null);
		assert(owner instanceof Client);
	}
	
	// Precondition and postcondition verifications are delegated to the ClientPublishingServiceConnector

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		((PublishingCI)this.getConnector()).publish(receptionPortURI, channel, message);
		
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		((PublishingCI)this.getConnector()).publish(receptionPortURI, channel, messages);
		
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInbounhdPortURI) throws Exception {
		((PublishingCI)this.getConnector()).asyncPublishAndNotify(receptionPortURI, channel, message, notificationInbounhdPortURI);
		
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInbounhdPortURI) throws Exception {
		((PublishingCI)this.getConnector()).asyncPublishAndNotify(receptionPortURI, channel, messages, notificationInbounhdPortURI);
	}
}