package fr.sorbonne_u.broker.component;

import fr.sorbonne_u.broker.component.tasks.SenderTask;
import fr.sorbonne_u.broker.ports.ReceivingOutboundPort;
import fr.sorbonne_u.components.AbstractComponent.AbstractTask;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * Class existing only in order to give an access to the client sending method : 
 * {@link ClientBrokerInfos#sendMessages(String, MessageI[])}.
 * 
 * <p>For the sending task implementation, please see concrete {@link SenderTask} class.</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true} // no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public abstract class VisibilitySenderTask extends AbstractTask {

	/**
	 * Send {@code messages} from {@code channel}, by delegating to the corresponding client
	 * {@link ReceivingOutboundPort} IF the client is on the same JVM (this.clientOutboundPort != null).
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.size() > 0}
	 * pre	{@code Stream.of(messages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param client						the client having the sending method.
	 * @param channel						name of the channel from which {@code messsages} are received.
	 * @param messages						array of messages received by the target client component.
	 * @return								true if {@code messages} have been sent, otherwise false.
	 * @throws IllegalArgumentException		if client is null.
	 * @throws Exception					if an error occurs while sending messages.
	 */
	protected boolean sendMessages(ClientBrokerInfos client, String channel, MessageI[] messages) throws Exception {
		if (client == null)
			throw new IllegalArgumentException("client cannot be null");
		
		// Other preconditions are delegated to the ClientBrokerInfos sendMessages method
		return client.sendMessages(channel, messages);
	}
}
