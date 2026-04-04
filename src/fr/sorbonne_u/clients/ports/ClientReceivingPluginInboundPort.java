package fr.sorbonne_u.clients.ports;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * Inbound port of a {@link Client}, allowing the handling of messages reception.<br>
 * Redirects messages received to the {@link ClientSubscriptionPlugin}, by either completing futures waiting those messages into
 * the plugin, or by delegating the reception to the {@code receive} plugin method (through {@code runTask}).
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
	 * @param uri    		the URI of the port.
	 * @param owner  		the component owning the port.
	 * @param pluginURI		the URI of the {@link ClientSubscriptionPlugin}.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public ClientReceivingPluginInboundPort(String uri, ComponentI owner, String pluginURI) throws Exception {
		super(uri, ReceivingCI.class, owner, pluginURI, null);
		// Precondition checks
		assert(uri != null && !uri.isEmpty());
		assert(owner != null);
		assert(owner instanceof Client);
		assert(pluginURI != null && !pluginURI.isEmpty());
	}

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		assert(message != null);
		
		ClientSubscriptionPlugin csp = (ClientSubscriptionPlugin) this.getOwnerPlugin(this.pluginURI);
		ConcurrentHashMap<String, ConcurrentLinkedQueue<CompletableFuture<MessageI>>> futureQueues = csp.getFutureQueues();
		
		// Locks the whole map to ensure that the future queue for the channel exists
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = csp.getOrCreateChannelFutureQueue(futureQueues, channel);
		
		// If there is a future waiting a message into the channel future queue : completes and retrieves the future from the queue
		// Otherwise : redirects the message to the the appropriate 'receive' method
		CompletableFuture<MessageI> future = channelFutureMessagesQueue.poll();
		if (future != null) {
			future.complete(message);
		} else {
			// Delegation through runTask for 'receive' method
			this.getOwner().runTask(client -> csp.receive(channel, message));
		}
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		assert(messages != null && messages.length > 0);
		assert(Stream.of(messages).allMatch(m -> m != null));
		
		ClientSubscriptionPlugin csp = (ClientSubscriptionPlugin) this.getOwnerPlugin(this.pluginURI);
		ConcurrentHashMap<String, ConcurrentLinkedQueue<CompletableFuture<MessageI>>> futureQueues = csp.getFutureQueues();
		
		// Locks the whole map to ensure that the future queue for the channel exists
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = csp.getOrCreateChannelFutureQueue(futureQueues, channel);
		
		// While there is still a future waiting a message into the channel future queue : completes and retrieves the futures from the queue
		// When there is no more futures : redirects remaining messages to consider to the the appropriate 'receive' method
		for (int i=0; i<messages.length; i++) {
			MessageI message = messages[i];
			
			CompletableFuture<MessageI> future = channelFutureMessagesQueue.poll();
			if (future != null) {
				future.complete(message);
			} else {
				MessageI[] remainingMessages = new MessageI[messages.length-i];
				for (int j=i; j<messages.length; j++) {
					remainingMessages[j-i] = messages[j];
				}
				
				// Delegation through runTask for 'receive' method
				if (remainingMessages.length == 1) {
					this.getOwner().runTask(client -> csp.receive(channel, remainingMessages[0]));
				} else {
					this.getOwner().runTask(client -> csp.receive(channel, remainingMessages));
				}
				break;
			}
		}
	}
}