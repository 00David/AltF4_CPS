package fr.sorbonne_u.clients.plugins;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.ports.ClientReceivingPluginInboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientSubscriptionI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The {@link Client} plugin having a {@link ClientReceivingPluginInboundPort}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
 * invariant	{@code registrationOutboundPort != null}
 * invariant	{@code futureMessages != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientSubscriptionPlugin extends AbstractPlugin implements ClientSubscriptionI {

	private static final long serialVersionUID = -1472841763925529259L;
	
	// RECEPTION
	
	/** {@link Client} reception inbound port URI */
	protected final String receivingInboundPortURI;
	
	/** Reception inbound port, set up after plugin installation + initialization */
	protected ClientReceivingPluginInboundPort receivingInboundPort;
	
	// REGISTRATION
	
	/** Registration outbound port, owned by {@link ClientRegistrationPlugin} */
	protected final ClientRegistrationOutboundPort registrationOutboundPort;
	
	/** Mapping : channel name -> a queue of futures, each waiting a message from the channel. */
	protected final ConcurrentHashMap<String, ConcurrentLinkedQueue<CompletableFuture<MessageI>>> futureQueues = new ConcurrentHashMap<>();
	
	/**
	 * Creates a {@link Client} subscription + reception plugin.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationOutboundPort != null}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI		the client inbound port URI offering {@link ReceivingCI} interface, used for creating the {@link ClientReceivingPluginInboundPort}.
	 * @param registrationOutboundPort		the client outbound port requiring {@link RegistrationCI} interface.
	 */
	public ClientSubscriptionPlugin(String receivingInboundPortURI, ClientRegistrationOutboundPort registrationOutboundPort){
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()) : new PreconditionException("receivingInboundPortURI cannot be null or empty");
		assert(registrationOutboundPort != null) : new PreconditionException("registrationOutboundPort cannot be null");
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationOutboundPort = registrationOutboundPort;
	}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addOfferedInterface(ReceivingCI.class);

	}
	
	@Override
	public void	initialise() throws Exception {
		this.receivingInboundPort = new ClientReceivingPluginInboundPort(receivingInboundPortURI, this.getOwner(), this.getPluginURI());
		this.receivingInboundPort.publishPort();
		super.initialise();
	}
	
	@Override
	public void uninstall() throws Exception {
		this.receivingInboundPort.unpublishPort();
		this.removeOfferedInterface(ReceivingCI.class);
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------

	// Precondition and postcondition verifications are delegated to the port used
	// except for reception methods (on receivingInboundPort)
	
	@Override
	public boolean channelExist(String channel) {
		try {
			return registrationOutboundPort.channelExist(channel);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean channelAuthorised(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return registrationOutboundPort.channelAuthorised(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean subscribed(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return registrationOutboundPort.subscribed(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void subscribe(String channel, MessageFilterI filter)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			registrationOutboundPort.subscribe(receivingInboundPortURI, channel, filter);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void unsubscribe(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		try {
			registrationOutboundPort.unsubscribe(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException | NotSubscribedChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void modifyFilter(String channel, MessageFilterI filter) throws UnknownClientException,
			UnknownChannelException, UnauthorisedClientException, NotSubscribedChannelException {
		try {
			registrationOutboundPort.modifyFilter(receivingInboundPortURI, channel, filter);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException | NotSubscribedChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Receive passively {@code message} from {@code channel}; the call is
	 * executed asynchronously to free the client component thread as soon as
	 * the corresponding task is submitted to the receiver.
	 * 
	 * <p>
	 * Needs to be @Override for implementing a more personalized behavior on reception.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel	name of an existing channel.
	 * @param message	message to be received from {@code channel}.
	 */
	@Override
	public void receive(String channel, MessageI message) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		
		Client client = (Client)this.getOwner();
		client.logMessage("------------------------------\n"
				+ "Thread "+Thread.currentThread().getId()+": Client received message '" + message + "' from the channel : " + channel 
				+ "\n------------------------------");
	}
	
	/**
	 * Receive passively {@code messages} from {@code channel}; the call is
	 * executed asynchronously to free the client component thread as soon as
	 * the corresponding task is submitted to the receiver.
	 * 
	 * <p>
	 * Needs to be @Override for implementing a more personalized behavior on reception.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.length > 0}
	 * pre	{@code Stream.of(messages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel		name of the channel from which {@code messsage} is received.
	 * @param messages		array of messages received by the component.
	 */
	@Override
	public void receive(String channel, MessageI[] messages) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		assert(messages != null && messages.length > 0);
		assert(Stream.of(messages).allMatch(m -> m != null));
		
		Client client = (Client)this.getOwner();
		client.logMessage("------------------------------\n"
				+ "Thread "+Thread.currentThread().getId()+": Client received "+ messages.length +" messages from the channel : " + channel 
				+ "\n------------------------------");
		/*for (int i=0; i<messages.length; i++) {
			MessageI message = messages[i];
			client.logMessage("Message " + i + " '" + message + "'");
		}*/
	}

	@Override
	public MessageI waitForNextMessage(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		// Precondition check
		assert(channel != null && !channel.isEmpty());
		
		Client client = (Client)this.getOwner();
		client.logMessage("Thread "+Thread.currentThread().getId()+": Waiting next message from channel '" + channel + "'");
		
		// Locks the whole map to ensure that the future queue for the channel exists
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = getOrCreateChannelFutureQueue(futureQueues, channel);
		
		// Creates and inserts the future into the queue
		CompletableFuture<MessageI> future = new CompletableFuture<>();
		channelFutureMessagesQueue.add(future);
		
		try {
			MessageI message =  future.get(); // Returns its result when got (the future is removed from the queue by the thread completing it)
			client.logMessage("------------------------------\n"
					+ "Thread "+Thread.currentThread().getId()+": Client waited and got message '" + message + "' from the channel : " + channel 
					+ "\n------------------------------");
			return message;
		} catch (InterruptedException | ExecutionException e) {
			channelFutureMessagesQueue.remove(future); // If an error occurs, removes the future from the queue if present in it
			client.logMessage("------------------------------\n"
					+ "Thread "+Thread.currentThread().getId()+": Client waited and got an error : " + e.getMessage() 
					+ "\n------------------------------");
			return null;
		}
	}
	
	
	@Override
	public MessageI waitForNextMessage(String channel, Duration d) throws UnknownClientException,
			UnknownChannelException, UnauthorisedClientException, NotSubscribedChannelException {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		assert(d != null);
		assert(!d.isZero());
		assert(!d.isNegative());
		
		Client client = (Client)this.getOwner();
		client.logMessage("Thread "+Thread.currentThread().getId()+": Waiting next message with a timeout ("+d.getSeconds()+"s) from channel '" + channel + "'");
		
		// Locks the whole map to ensure that the future queue for the channel exists
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = getOrCreateChannelFutureQueue(futureQueues, channel);
		
		// Creates and inserts the future into the queue
		CompletableFuture<MessageI> future = new CompletableFuture<>();
		channelFutureMessagesQueue.add(future);
		
		try {
			MessageI message =  future.get(d.getSeconds(), TimeUnit.SECONDS); // Returns its result when got (the future is removed from the queue by the thread completing it)
			client.logMessage("------------------------------\n"
					+ "Thread "+Thread.currentThread().getId()+": Client waited and got message '" + message + "' from the channel : " + channel 
					+ "\n------------------------------");
			return message;
		} catch (TimeoutException e) {
			channelFutureMessagesQueue.remove(future); // If the timeout has expired, removes the future from the queue
			client.logMessage("------------------------------\n"
					+ "Thread "+Thread.currentThread().getId()+": Client waited but timeout has expired"
					+ "\n------------------------------");
			return null;
		} catch (InterruptedException | ExecutionException e) {
			channelFutureMessagesQueue.remove(future); // If an error, removes the future from the queue if present in it
			client.logMessage("------------------------------\n"
					+ "Thread "+Thread.currentThread().getId()+": Client waited and got an error : " + e.getMessage() 
					+ "\n------------------------------");
			return null;
		}
	}

	@Override
	public Future<MessageI> getNextMessage(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		// Precondition check
		assert(channel != null && !channel.isEmpty());
		
		Client client = (Client)this.getOwner();
		client.logMessage("Thread "+Thread.currentThread().getId()+": Creates a message future for the channel '" + channel + "'");
		
		// Locks the whole map to ensure that the future queue for the channel exists
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = getOrCreateChannelFutureQueue(futureQueues, channel);
		
		// Creates and inserts the future into the queue
		CompletableFuture<MessageI> future = new CompletableFuture<>();
		channelFutureMessagesQueue.add(future);
		
		// Postcondition verified by construction 
		return future;
	}
	
	// -------------------------------------------------------------------------
	// Plug-in getters for the future queues
	// -------------------------------------------------------------------------
	
	/**
	 * Returns a mapping : channel name -> a queue of futures, each future waiting a message from the channel.<br>
	 * Used by the {@link ClientReceivingPluginInboundPort}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * @return	a mapping : channel name -> a queue of futures, each future waiting a message from the channel.
	 */
	public ConcurrentHashMap<String, ConcurrentLinkedQueue<CompletableFuture<MessageI>>> getFutureQueues() {
		return futureQueues;
	}
	
	/**
	 * Returns a queue of futures, each future waiting a message from the {@code channel}.<br>
	 * Used by the {@link ClientReceivingPluginInboundPort}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code futureQueues != null}
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * @param futureQueues		a queue of futures, each future waiting a message from the {@code channel}.
	 * @param channel			name of the channel from which a message is waited to be received.
	 * 
	 * @return	a mapping : channel name -> a queue of futures, each future waiting a message from the channel.
	 */
	public ConcurrentLinkedQueue<CompletableFuture<MessageI>> getOrCreateChannelFutureQueue
		(ConcurrentHashMap<String, ConcurrentLinkedQueue<CompletableFuture<MessageI>>> futureQueues, String channel) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		assert(futureQueues != null);
		
		synchronized(futureQueues) {
			ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = futureQueues.get(channel);
			if (channelFutureMessagesQueue == null) {
				channelFutureMessagesQueue = new ConcurrentLinkedQueue<>();
				futureQueues.put(channel, channelFutureMessagesQueue);
			}
			
			// Postcondition verified by construction 
			return channelFutureMessagesQueue;
		}
	}
	
}
