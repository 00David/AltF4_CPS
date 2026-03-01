package fr.sorbonne_u.clients.connectors;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;

/**
 * Connector between an outbound port of the {@link Client} component and an inbound port of a {@link Broker}.<br>
 * Delegates message publication to the {@link BrokerPublishingInboundPort}
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientPublishingServiceConnector extends AbstractConnector implements PublishingCI{
	
	// Precondition and postcondition verifications are delegated to the the BrokerPublishingInboundPort
	
	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		((PublishingCI)this.offering).publish(receptionPortURI, channel, message);
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		((PublishingCI)this.offering).publish(receptionPortURI, channel, messages);
	}
}