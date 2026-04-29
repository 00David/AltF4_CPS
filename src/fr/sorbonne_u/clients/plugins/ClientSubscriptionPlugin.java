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

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.ports.ClientReceivingInboundPort;
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
 * The {@link Client} plug-in having a {@link ClientReceivingInboundPort}.
 * 
 * <p>
 * Any concrete {@link Client} must, in its constructor, set up either this ClientSubscriptionPlugin
 * using the {@link Client#setupSubscriptionPlugin} method, or any other subclass with an equivalent setup.
 * </p>
 * 
 * <p>
 * Despite having the possibility of being abstract to let concrete sub-classes implement a personalized behavior
 * on standard reception methods, this is not the case because the contained {@link ClientReceivingInboundPort}
 * must necessarily be published for just registering on a {@link Broker}, without even having to deal with receptions.
 * <p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code registrationPlugin != null && registrationPlugin.isInitialised()}
 * invariant	{@code futureMessages != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientSubscriptionPlugin extends AbstractPlugin implements ClientSubscriptionI {

	private static final long serialVersionUID = -1472841763925529259L;

	/** 
	 * Registration plug-in, owning an outbound port requiring {@link RegistrationCI} interface. 
	 * Some local services are delegated to it.
	 */
	protected final ClientRegistrationPlugin registrationPlugin;
	
	// RECEPTION
	
	/** Reception inbound port, set up after plugin installation + initialization */
	protected ClientReceivingInboundPort receivingInboundPort;
	
	/** Mapping : channel name -> a queue of futures, each waiting a message from the channel. */
	protected final ConcurrentHashMap<String, ConcurrentLinkedQueue<CompletableFuture<MessageI>>> futureQueues = new ConcurrentHashMap<>();
	
	/**
	 * Creates a {@link Client} subscription + reception plug-in.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationPlugin != null && registrationPlugin.isInitialised()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param registrationPlugin		The client registration plug-in owning an outbound port requiring {@link RegistrationCI} interface.
	 */
	public ClientSubscriptionPlugin(ClientRegistrationPlugin registrationPlugin){
		// Precondition checks
		assert(registrationPlugin != null  && registrationPlugin.isInitialised()) : 
			new PreconditionException("registrationPlugin must be initialised");
		
		this.registrationPlugin = registrationPlugin;
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
		super.initialise();
		Client owner = (Client)this.getOwner();
		this.receivingInboundPort = new ClientReceivingInboundPort(owner.receivingInboundPortURI, (Client)this.getOwner(), this.getPluginURI());
		this.receivingInboundPort.publishPort();
	}
	
	@Override
	public void uninstall() throws Exception {
		this.receivingInboundPort.unpublishPort();
		this.removeOfferedInterface(ReceivingCI.class);
		super.uninstall();
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------

	// Precondition and postcondition verifications are delegated to the registration plug-in used,
	// except for reception methods (on receivingInboundPort)
	
	@Override
	public boolean channelExist(String channel) {
		try {
			return registrationPlugin.channelExist(channel);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean channelAuthorised(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return registrationPlugin.channelAuthorised(channel);
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
			return registrationPlugin.subscribed(channel);
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
			registrationPlugin.subscribe(channel, filter);
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
			registrationPlugin.unsubscribe(channel);
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
			registrationPlugin.modifyFilter(channel, filter);
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
		
		// Gets the future queue for the channel
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = getOrCreateChannelFutureQueue(channel);
		
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
		
		// Gets the future queue for the channel
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = getOrCreateChannelFutureQueue(channel);
		
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
		
		// Gets the future queue for the channel
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = getOrCreateChannelFutureQueue(channel);
		
		// Creates and inserts the future into the queue
		CompletableFuture<MessageI> future = new CompletableFuture<>();
		channelFutureMessagesQueue.add(future);
		
		// Postcondition verified by construction 
		return future;
	}
	
	// -------------------------------------------------------------------------
	// Internal helper method
	// -------------------------------------------------------------------------
	
	/**
	 * Returns a multi-thread safe queue, able to contain futures waiting to be completed 
	 * on message reception from the desired {@code channel}.<br>
	 * If the queue does not already exist, it is created.
	 * 
	 * <p>
	 * This method locks the whole {@code this.futureQueues} map, to ensure that the future 
	 * queue for the channel exists.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code return != null}
	 * </pre>

	 * @param channel			name of the channel from which a message is waited to be received.
	 * @return					a multi-thread safe queue that may contain futures waiting a message from the {@code channel}.
	 */
	private ConcurrentLinkedQueue<CompletableFuture<MessageI>> getOrCreateChannelFutureQueue(String channel) {
		// Precondition check
		assert(channel != null && !channel.isEmpty());
		
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
	
	// -------------------------------------------------------------------------
	// Plug-in reception handling
	// -------------------------------------------------------------------------

	/**
	 * Handles the reception of incoming messages from the given {@code channel}.
	 * <p>
	 * The method gets the associated future queue and processes messages as follows :
	 * </p>
	 * <ul>
	 *   <li>
	 *     While futures are waiting in the queue, each incoming message is used to
	 *     complete the next future, which is then removed from the queue.
	 *   </li>
	 *   <li>
	 *     Once no more futures are waiting, the remaining messages are forwarded
	 *     to the appropriate {@code receive} method.
	 *   </li>
	 * </ul>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.size() > 0}
	 * pre	{@code Stream.of(messages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel		name of the channel from which {@code messsage} is received.
	 * @param messages		array of messages received by the component.
	 */
	public void handleIncomingReception(String channel, MessageI[] messages) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		assert(messages != null && messages.length > 0);
		assert(Stream.of(messages).allMatch(m -> m != null));

		// Gets the future queue for the channel
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> channelFutureMessagesQueue = getOrCreateChannelFutureQueue(channel);

		for (int i=0; i<messages.length; i++) {
			MessageI message = messages[i];
			
			// Removes the next future from the queue
			CompletableFuture<MessageI> future = channelFutureMessagesQueue.poll();
			
			if (future != null) { // If it is not null : completes it
				future.complete(message);
			
			} else { // Else : redirects the remaining incoming messages to a receive method
				MessageI[] remainingMessages = new MessageI[messages.length-i];
				for (int j=i; j<messages.length; j++) {
					remainingMessages[j-i] = messages[j];
				}
				
				// Delegation through runTask
				if (remainingMessages.length == 1) {
					this.getOwner().runTask(client -> receive(channel, remainingMessages[0]));
				} else {
					this.getOwner().runTask(client -> receive(channel, remainingMessages));
				}
				break;
			}
		}
	}
	
}
