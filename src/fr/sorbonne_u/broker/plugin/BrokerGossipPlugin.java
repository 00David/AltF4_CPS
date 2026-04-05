package fr.sorbonne_u.broker.plugin;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.connectors.BrokerGossipServiceConnector;
import fr.sorbonne_u.broker.ports.BrokerGossipPluginInboundPort;
import fr.sorbonne_u.broker.ports.BrokerGossipPluginOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipImplementationI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipReceiverCI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.GossipMessage;

/**
 * The {@link Broker} plugin having a {@link BrokerGossipPluginInboundPort} and a {@link BrokerGossipPluginOutboundPort}.
 * 
 * <p>Handles {@link GossipMessage} reception and sending</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code gossipInboundPort != null && !gossipInboundPort.isEmpty()}
 * invariant	{@code gossipOutboundPorts != null}
 * invariant	{@code gossipMessages != null}
 * invariant	{@code gossipMessagesLock != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class BrokerGossipPlugin extends AbstractPlugin implements GossipImplementationI {

	private static final long serialVersionUID = -3119414073600533953L;
	
	/** A pair corresponding to an outbound gossip connection : (current Broker outbound port, another Broker inbound port URI). */
	private static class OutboundConnection{
		public OutboundConnection(BrokerGossipPluginOutboundPort gossipOutboundPort, String portToConnectURI) {
			this.gossipOutboundPort = gossipOutboundPort;
			this.portToConnectURI = portToConnectURI;
		}
		private BrokerGossipPluginOutboundPort gossipOutboundPort;
		private final String portToConnectURI;
	}
	
	// GOSSIP PROTOCOL PORTS
	
	/** {@link Broker} gossip inbound port URI. */
	protected final String gossipInboundPortURI;
	
	/** Gossip message reception inbound port, set up after plugin installation + initialization. */
	protected BrokerGossipPluginInboundPort gossipInboundPort;
	
	/** 
	 * A mapping : {@link Broker} gossip outbound port URI -> the corresponding gossip connection.
	 * Each outound port of a connection is set up after plugin installation + initialization.<br>
	 * Considering the current static architecture, outbound ports will not change during execution, 
	 * and thus there is no need for multi-thread protection in that case.
	 */
	protected final Map<String, OutboundConnection> gossipOutboundPorts = new HashMap<>();
	
	// GOSSIP PROTOCOL INFORMATIONS
	
	/** List of received {@link GossipMessage}s. To prevent simultaneous multi-thread accesses, its {@code gossipMessagesLock} is used. */
	protected final Set<GossipMessage> gossipMessages = new HashSet<>();
	
	/** {@code gossipMessages} lock. */
	protected final ReentrantReadWriteLock gossipMessagesLock = new ReentrantReadWriteLock();
	
	/** 
	 * If the number of stored treated messages ({@code this.gossipMessages}) exceeds the threshold, 
	 * gossip messages that were created since more than {@code MAX_SECONDS_DIFF} will be deleted.
	 */
	public static final int GOSSIP_MESSAGES_THRESHOLD = 1000;
	
	/** Gossip messages created since more than 120 seconds might be deleted. */
	public static final int MAX_SECONDS_DIFF = 120;

	/**
	 * Creates a {@link Broker} gossip message handling plugin.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code gossipInboundPortURI != null && !gossipInboundPortURI.isEmpty()}
	 * pre	{@code gossipOutboundPortURIs != null}
	 * pre	{@code portsToConnect != null}
	 * pre	{@code gossipOutboundPortURIs.size() == portsToConnect.size()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param gossipInboundPortURI		The {@link Broker} inbound port URI offering {@link GossipReceiverCI} interface.
	 * @param gossipOutboundPortURIs	{@link Broker} outbound port URIs requiring {@link GossipSenderCI} interface.
	 * @param otherBrokersToConnectURIs	Other {@link Broker}s gossip inbound port URIs, a URI at an index i in {@code gossipOutboundPortURIs} 
	 * 									connects to the port URI with the same index in {@code portsToConnect}.
	 */
	public BrokerGossipPlugin(String gossipInboundPortURI, List<String> gossipOutboundPortURIs, List<String> otherBrokersToConnectURIs){
		// Precondition checks
		assert(gossipInboundPortURI != null && !gossipInboundPortURI.isEmpty()) : new PreconditionException("gossipInboundPortURI cannot be null or empty");
		assert(gossipOutboundPortURIs != null) : new PreconditionException("gossipOutboundPortURIs cannot be null");
		assert(otherBrokersToConnectURIs != null) : new PreconditionException("otherBrokersToConnectURIs cannot be null");
		assert(gossipOutboundPortURIs.size() == otherBrokersToConnectURIs.size()) : new PreconditionException("both lists must have the same size");
		
		this.gossipInboundPortURI = gossipInboundPortURI;
		for (int i=0; i<gossipOutboundPortURIs.size(); i++) {
			String gossipOutboundPortURI = gossipOutboundPortURIs.get(i);
			String portToConnect = otherBrokersToConnectURIs.get(i);
			gossipOutboundPorts.put(gossipOutboundPortURI, new OutboundConnection(null, portToConnect));
		}
	}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addOfferedInterface(GossipReceiverCI.class);
		this.addRequiredInterface(GossipSenderCI.class);
	}
	
	@Override
	public void	initialise() throws Exception {
		this.gossipInboundPort = new BrokerGossipPluginInboundPort(gossipInboundPortURI, this.getOwner(), "gossip");
		this.gossipInboundPort.publishPort();
		
		// Each outbound port is connected to the corresponding inbound port of another Broker, with an update of 'OutboundConnection' objects
		for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
			String gossipOutboundPortURI = e.getKey();
			OutboundConnection connection = e.getValue();
			
			BrokerGossipPluginOutboundPort gossipOutboundPort = new BrokerGossipPluginOutboundPort(gossipOutboundPortURI, this.getOwner());
			connection.gossipOutboundPort = gossipOutboundPort;
			gossipOutboundPort.publishPort();
			
			this.getOwner().doPortConnection(gossipOutboundPort.getPortURI(),
					connection.portToConnectURI,
					BrokerGossipServiceConnector.class.getCanonicalName());
		}

		super.initialise();
	}
	
	@Override
	public void	finalise() throws Exception {
		// Each outbound port is disconnected
		for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
			OutboundConnection connection = e.getValue();
			this.getOwner().doPortDisconnection(connection.gossipOutboundPort.getPortURI());
		}
	}
	
	@Override
	public void uninstall() throws Exception {
		this.gossipInboundPort.unpublishPort();
		this.removeOfferedInterface(GossipReceiverCI.class);
		
		// Each outbound port is unpublished
		for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
			OutboundConnection connection = e.getValue();
			connection.gossipOutboundPort.unpublishPort();
		}
		this.removeRequiredInterface(GossipSenderCI.class);
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------
	
	/**
	 * Marks the given gossip messages as locally treated (by putting them into {@code this.gossipMessages}) 
	 * and sends them to another(s) {@link Broker}'s {@link BrokerGossipPluginInboundPort}.
	 * 
	 * <p>Messages will be sent to Brokers to which {@code gossipOutboundPort}s are currently connected.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	all messages in {@code gossipMessages} have been locally treated in the {@link Broker} owner. // informal
	 * pre	{@code gossipMessages != null && gossipMessages.length > 0}
	 * pre	{@code Stream.of(gossipMessages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param gossipMessages	gossip messages sent by the sender in the gossip protocol.
	 */
	public void send(GossipMessageI[] gossipMessages){
		// Precondition check
		assert(gossipMessages != null && gossipMessages.length > 0) : new PreconditionException("gossipMessages cannot be null or empty");
		assert(Stream.of(gossipMessages).allMatch(m -> m != null)) : new PreconditionException("a gossip message cannot be null");
		
		// Keeps the write lock during the whole process of marking locally treated messages.
		gossipMessagesLock.writeLock().lock();
		try {
			for (int i=0; i<gossipMessages.length; i++) {
				if (gossipMessages[i] instanceof GossipMessage) {
					GossipMessage gossipMessage = (GossipMessage)gossipMessages[i];
					this.gossipMessages.add(gossipMessage);
				} else {
					System.err.println("Thread "+Thread.currentThread().getId()+": Gossip sending error : message not an instance of GossipMessage");
				}
			}
		} finally {
			gossipMessagesLock.writeLock().unlock();
		}
		
		try {
			for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
				BrokerGossipPluginOutboundPort gossipOutboundPort = e.getValue().gossipOutboundPort;
				gossipOutboundPort.send(gossipMessages);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Receives an array of gossip messages from another {@link Broker}, keeps gossip messages not marked as treated locally 
	 * and delegates Broker updates to the {@code update} method.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code gossipMessages != null && gossipMessages.length > 0}
	 * pre	{@code Stream.of(gossipMessages).allMatch(m -> m != null && m instanceof GossipMessage)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param gossipMessages	gossip messages received from the sender in the gossip protocol.
	 */
	@Override
	public void receive(GossipMessageI[] gossipMessages) throws Exception {
		// Precondition check
		assert(gossipMessages != null && gossipMessages.length > 0) : new PreconditionException("gossipMessages cannot be null or empty");
		assert(Stream.of(gossipMessages).allMatch(m -> m != null && m instanceof GossipMessage)) : new PreconditionException("Must contain only non null GossipMessage");
		
		Broker broker = (Broker)this.getOwner();
		broker.logMessage("Thread "+Thread.currentThread().getId()+": "+gossipMessages.length+" gossip message(s) received");
		update(keepNotTreatedMessages(gossipMessages));
	}

	/**
	 * Considers the given non null gossip messages received from another sender {@link Broker}, the current thread will :
	 * 
	 * <ul>
	 * 	<li>Delegate the associated action of each gossip message {@code fromSender} to another {@link Broker} thread,
	 * 			then the designated thread will call at the end of its Broker service the {@code send} method, marking the
	 * 			gossip message as locally treated and propagating the gossip message to other connected Brokers.</li>
	 * 	<li>Eventually clean old treated messages in {@code this.gossipMessages}, if the number of messages exceeds a certain threshold
	 * 			and some messages exceed maximum storage time.</li>
	 * </ul>
	 * 
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code fromSender != null && fromSender.length > 0}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param fromSender	gossip messages received from the sender in the gossip protocol.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void update(GossipMessageI[] fromSender) {
		// Precondition check
		assert(fromSender != null && fromSender.length > 0) : new PreconditionException("fromSender cannot be null or empty");
		
		List<GossipMessageI> messagesToSendToNeighbours = new ArrayList<>();
		
				
		// Delegate the associated action of each gossip message {@code fromSender} to another {@link Broker} thread
		for (int i=0; i<fromSender.length; i++) {
			if (fromSender[i] instanceof GossipMessage) {
				GossipMessage gossipMessage = (GossipMessage)fromSender[i];
				messagesToSendToNeighbours.add(gossipMessage);
				MessageI parameters = gossipMessage.message();
				try {
					this.getOwner().handleRequest( owner -> {
						Broker broker = (Broker)owner;
						GossipMessageI messageCopy = gossipMessage.copyWithNewEmitterURI(broker.getReflectionInboundPortURI());
						switch (gossipMessage.messageAction()) {
							case CREATE_CHANNEL:
								broker.createChannelService((String)parameters.getPropertyValue("receptionPortURI"), 
										(String)parameters.getPropertyValue("channel"),
										(String)parameters.getPropertyValue("autorizedUsers"),
										messageCopy);
								break;
							case DESTROY_CHANNEL:
								boolean now = (boolean)parameters.getPropertyValue("now");
								if (now) {
									broker.destroyChannelNowService((String)parameters.getPropertyValue("receptionPortURI"), 
											(String)parameters.getPropertyValue("channel"),
											messageCopy);
								} else {
									broker.destroyChannelService((String)parameters.getPropertyValue("receptionPortURI"), 
											(String)parameters.getPropertyValue("channel"),
											messageCopy);
								}
								break;
							case PUBLISH:
								broker.publishService((String)parameters.getPropertyValue("receptionPortURI"), 
										(String)parameters.getPropertyValue("channel"),
										(ArrayList<MessageI>)parameters.getPayload(),
										messageCopy);
								break;
							case REGISTER_CLIENT:
								broker.registerService((String)parameters.getPropertyValue("receptionPortURI"), 
										(RegistrationClass)parameters.getPropertyValue("rc"),
										messageCopy);
								break;
							case SUBSCRIBE_CLIENT_CHANNEL:
								broker.subscribeService((String)parameters.getPropertyValue("receptionPortURI"), 
										(String)parameters.getPropertyValue("channel"),
										(MessageFilterI)parameters.getPropertyValue("filter"),
										messageCopy);
								break;
							case UNREGISTER_CLIENT:
								broker.unregisterService((String)parameters.getPropertyValue("receptionPortURI"), 
										messageCopy);
								break;
							case UNSUBSCRIBE_CLIENT_CHANNEL:
								broker.unsubscribeService((String)parameters.getPropertyValue("receptionPortURI"), 
										(String)parameters.getPropertyValue("channel"),
										messageCopy);
								break;
							case UPDATE_CHANNEL_AUTHORIZED_URIS:
								broker.modifyAuthorisedUsersService((String)parameters.getPropertyValue("receptionPortURI"), 
										(String)parameters.getPropertyValue("channel"),
										(String)parameters.getPropertyValue("autorizedUsers"),
										messageCopy);
								break;
							case UPDATE_CLIENT_SERVICE_CLASS:
								broker.modifyServiceClassService((String)parameters.getPropertyValue("receptionPortURI"), 
										(RegistrationClass)parameters.getPropertyValue("rc"),
										messageCopy);
								break;
							default: // case NOTHING
								break;
						}
						return null;
					});
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else {
				System.err.println("Thread "+Thread.currentThread().getId()+": Gossip update error : message not an instance of GossipMessage");
			}
		}
		
		gossipMessagesLock.writeLock().lock();
		try {
			
			// Eventually clean old treated messages
			if (this.gossipMessages.size() > GOSSIP_MESSAGES_THRESHOLD) {
				Instant iNow = Instant.now();
				this.gossipMessages.removeIf(msg -> {
					Duration duration = Duration.between(msg.timestamp(), iNow);
					return duration.toMillis() > MAX_SECONDS_DIFF;
				});
			}
			
		} finally {
			gossipMessagesLock.writeLock().unlock();
		}
		
	}
	
	/**
	 * 'Cleans' the given gossip messages array, by returning a new array with only gossip messages 
	 * instances of {@link GossipMessage}, and not marked as treated here (not in {@code this.gossipMessages}).

	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code gossipMessagesToClean != null}
	 * post	{@code return != null && Stream.of(return).allMatch(m -> m != null && m instanceof GossipMessage)}
	 * </pre>
	 *
	 * @param gossipMessagesToClean		Gossip messages received from the sender in the gossip protocol.
	 * @return							A new array with only gossip messages not marked as treated here.
	 */
	private GossipMessageI[] keepNotTreatedMessages(GossipMessageI[] gossipMessagesToClean) {
		// Precondition check
		assert(gossipMessagesToClean != null) : new PreconditionException("gossipMessagesToClean cannot be null or empty");
		
		List<GossipMessage> messagesToKeep = new ArrayList<>(); // must be instances of 'GossipMessage'
		
		gossipMessagesLock.readLock().lock();
		try {
			for (int i=0; i<gossipMessagesToClean.length; i++) {
				if (gossipMessagesToClean[i] instanceof GossipMessage) {
					GossipMessage gossipMessage = (GossipMessage)gossipMessagesToClean[i];
					if (!(this.gossipMessages.contains(gossipMessage))) messagesToKeep.add(gossipMessage);
				} else {
					System.err.println("Thread "+Thread.currentThread().getId()+": Gossip error : message not an instance of GossipMessage");
				}
			}
		} finally {
			gossipMessagesLock.readLock().unlock();
		}
		
		GossipMessageI[] keptGossipMessages = new GossipMessageI[messagesToKeep.size()];
		for (int j=0; j<messagesToKeep.size(); j++) {
			keptGossipMessages[j] = messagesToKeep.get(j);
		}
		
		// Postcondition check
		assert(Stream.of(keptGossipMessages).allMatch(m -> m != null && m instanceof GossipMessage)) : new PostconditionException("Must contain only non null GossipMessage");
		return keptGossipMessages;
	}

}
