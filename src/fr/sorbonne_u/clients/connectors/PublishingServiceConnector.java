package fr.sorbonne_u.clients.connectors;

import java.util.ArrayList;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.clients.ports.PublishingOutboundPort;

/**
 * Connector between a {@link PublishingOutboundPort} and a port implementing PublishingCI, 
 * delegates message publication to this port.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PublishingServiceConnector extends AbstractConnector implements PublishingCI {
	
	// Precondition and postcondition verifications are delegated to the the port implementing PublishingCI
	
	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		((PublishingCI)this.offering).publish(receptionPortURI, channel, message);
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		((PublishingCI)this.offering).publish(receptionPortURI, channel, messages);
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInbounhdPortURI) throws Exception {
		((PublishingCI)this.offering).asyncPublishAndNotify(receptionPortURI, channel, message, notificationInbounhdPortURI);
		
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInbounhdPortURI) throws Exception {
		((PublishingCI)this.offering).asyncPublishAndNotify(receptionPortURI, channel, messages, notificationInbounhdPortURI);
	}
}