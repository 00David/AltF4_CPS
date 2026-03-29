package fr.sorbonne_u.clients.plugins;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
	
	//Queues for waiting the messages
	protected ConcurrentHashMap<String, BlockingQueue<MessageI>> messagesQueue = new ConcurrentHashMap<String, BlockingQueue<MessageI>>();
	protected ConcurrentHashMap<String, Queue<CompletableFuture<MessageI>>> futureQueues = new ConcurrentHashMap<String, Queue<CompletableFuture<MessageI>>>();
	
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
		enqueueMessage(channel, message);
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
	 * pre	{@code messages != null && messages.size() > 0}
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
		for (int i=0; i<messages.length; i++) {
			MessageI message = messages[i];
			enqueueMessage(channel, message);
			//client.logMessage("Message " + i + " '" + message + "'");
		}
	}

	@Override
	public MessageI waitForNextMessage(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		
		Client client = (Client)this.getOwner();
		client.logMessage("Waiting next message on channel " + channel);
		
		try {
			//Retrieve a message from the queue if queue is not empty 
			//else wait for a message is added in the queue
	        return messagesQueue.get(channel).take();
	    } catch (InterruptedException e) {
	        Thread.currentThread().interrupt();
	        return null;
	    }
	}
	
	
	@Override
	public MessageI waitForNextMessage(String channel, Duration d) throws UnknownClientException,
			UnknownChannelException, UnauthorisedClientException, NotSubscribedChannelException {
		
		Client client = (Client)this.getOwner();
		client.logMessage("Waiting next message on channel " + channel + "for " + d.getSeconds() + " sec");
		
		try {
			//Retrieve a message from the queue if queue is not empty 
			//else wait for x sec for a message is added in the queue
	        return messagesQueue.get(channel).poll(d.toMillis(), TimeUnit.MILLISECONDS);
	    } catch (InterruptedException e) {
	        Thread.currentThread().interrupt();
	        return null;
	    }
	}

	@Override
	public Future<MessageI> getNextMessage(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		
		//if message queue of the channel don't exists then add the queue.
		BlockingQueue<MessageI> q =
		        messagesQueue.computeIfAbsent(channel, k -> new LinkedBlockingQueue<>());
		
		Client client = (Client)this.getOwner();
		client.logMessage("Waiting future next message on channel " + channel);

		
			//Retrieve the head of the message queue if not empty 
			//else wait for a message to be added in the queue
		    MessageI msg = q.poll();
		    if (msg != null) {
		        return CompletableFuture.completedFuture(msg);
		    }
		    
		    //Create the future
		    CompletableFuture<MessageI> future = new CompletableFuture<>();

		    //Create the queue for the future message
		    Queue<CompletableFuture<MessageI>> fq =
		        futureQueues.computeIfAbsent(channel, k -> new LinkedList<>());

		    //add the future in the future message queue
		    fq.add(future);

		    return future;
	}
	
	
	/**
	 * Enqueue a message in the internal queues of {@code channel}
	 * <p><strong>Contract</strong></p>
		<pre>
		pre none
		post none
		</pre>
		@param channel name of the channel
		@param mes  message to enqueue
	 */
	public synchronized void enqueueMessage(String channel, MessageI mes) {
		
		System.out.println("enqueueMessage");
		
		//We prioritize the queue of future
	    Queue<CompletableFuture<MessageI>> fq = futureQueues.get(channel);
	    
	    if (fq != null && !fq.isEmpty()) {
	        fq.poll().complete(mes);
	    }

	    BlockingQueue<MessageI> q =
	        messagesQueue.computeIfAbsent(channel, k -> new LinkedBlockingQueue<>());

	    q.offer(mes);
	}
	
}
