package fr.sorbonne_u.clients.ports;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.PublishingServiceConnector;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Delegates publications to its {@link PublishingServiceConnector}.
 * 
 * <p>
 * Used as an outbound port of a {@link Client}, 
 * allowing the publication of one or more messages to a {@link Broker}.
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
public class PublishingOutboundPort extends AbstractOutboundPort implements PublishingCI{

	private static final long serialVersionUID = 2672780904734490388L;

	/**
	 * Creates the outbound port redirecting to a {@link PublishingServiceConnector}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code owner != null}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param owner  		the component owning this port.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public PublishingOutboundPort(ComponentI owner) throws Exception {
		super(PublishingCI.class, owner);
		// Precondition check
		assert(owner != null) : new PreconditionException("owner cannot be null");
	}
	
	/**
	 * Creates the outbound port redirecting to a {@link PublishingServiceConnector}.<br>
	 * Used by {@link PrivilegedOutboundPort} constructor.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code owner != null}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param implementedInterface    	the implemented interface, extending {@link RequiredCI}.
	 * @param owner  					the component owning this port.
	 * @throws Exception 				if an error occurs while creating the port
	 */
	public PublishingOutboundPort(Class<? extends RequiredCI> implementedInterface, ComponentI owner) throws Exception {
		super(implementedInterface, owner);
		// Precondition check
		assert(owner != null) : new PreconditionException("owner cannot be null");
	}
	
	// Precondition and postcondition verifications are delegated to the PublishingServiceConnector

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