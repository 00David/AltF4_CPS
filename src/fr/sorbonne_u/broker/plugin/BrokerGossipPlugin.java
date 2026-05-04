package fr.sorbonne_u.broker.plugin;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.connectors.GossipServiceConnector;
import fr.sorbonne_u.broker.ports.BrokerGossipInboundPort;
import fr.sorbonne_u.broker.ports.GossipOutboundPort;
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
import fr.sorbonne_u.messages.gossip.GossipMessage;

/**
 * The {@link Broker} plugin having a {@link BrokerGossipInboundPort} and a {@link GossipOutboundPort}.
 * 
 * <p>
 * Handles {@link GossipMessage} reception and sending, with each internal method being necessarily executed by a thread from 
 * the Broker {@code Broker.GOSSIP_POOL_URI} thread pool (except for the constructor and methods related to the plugin life cycle). 
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code gossipInboundPort != null && !gossipInboundPort.isEmpty()}
 * invariant	{@code gossipOutboundPorts != null}
 * invariant	{@code gossipMessages != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class BrokerGossipPlugin extends AbstractPlugin implements GossipImplementationI {

	private static final long serialVersionUID = -3119414073600533953L;
	
	/** A pair corresponding to an outbound gossip connection : (current Broker outbound port, another Broker inbound port URI). */
	private static class OutboundConnection{
		public OutboundConnection(GossipOutboundPort gossipOutboundPort, String portToConnectURI) {
			this.gossipOutboundPort = gossipOutboundPort;
			this.portToConnectURI = portToConnectURI;
		}
		private GossipOutboundPort gossipOutboundPort;
		private final String portToConnectURI;
	}
	
	// GOSSIP PROTOCOL PORTS
	
	/** {@link Broker} gossip inbound port URI. */
	protected final String gossipInboundPortURI;
	
	/** Gossip message reception inbound port, set up after plugin installation + initialization. */
	protected BrokerGossipInboundPort gossipInboundPort;
	
	/** 
	 * A mapping : {@link Broker} gossip outbound port URI -> the corresponding gossip connection.
	 * Each outound port of a connection is set up after plugin installation + initialization.<br>
	 * Considering the current static architecture, outbound ports will not change during execution, 
	 * and thus there is no need for multi-thread protection in that case.
	 */
	protected final Map<String, OutboundConnection> gossipOutboundPorts = new HashMap<>();
	
	// GOSSIP PROTOCOL INFORMATIONS
	
	/** 
	 * Set of received {@link GossipMessage}s. 
	 * 
	 * <p>
	 * This set is either expanded before sending local messages, via the {@link BrokerGossipPlugin#send(GossipMessageI[])} method,
	 * or on foreign messages reception, in the {@link BrokerGossipPlugin#receive(GossipMessageI[])} method.<br>
	 * If a received message is already present (via its message URI), it is not taken into account.
	 * </p>
	 * 
	 * <p>
	 * This set can also be reduced at the end of the {@link BrokerGossipPlugin#update(GossipMessageI[])} method, if
	 * old gossip messages are present. See the {@code GOSSIP_MESSAGES_THRESHOLD} and {@code MAX_SECONDS_DIFF} attributes.
	 * <p/>
	 * 
	 * <p>
	 * To prevent simultaneous multi-thread accesses, its synchronized(...) monitor lock is used.
	 * (every time the monitor lock is taken, it is for a potential write in the set) 
	 * </p>
	 */
	protected final Set<GossipMessage> receivedMessages = new HashSet<>();
	
	/** 
	 * If the number of stored received messages ({@code this.receivedMessages}) exceeds the threshold, 
	 * gossip messages that were created since more than {@code MAX_SECONDS_DIFF} will be deleted.
	 */
	public static final int GOSSIP_MESSAGES_THRESHOLD = 1000;
	
	/** Stored received gossip messages created since more than 120 seconds might be deleted. */
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
		super.initialise();
		this.gossipInboundPort = new BrokerGossipInboundPort(gossipInboundPortURI, (Broker)this.getOwner(), "gossip");
		this.gossipInboundPort.publishPort();
		
		// For each outbound connection, create and publish the outbound port
		for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
			String gossipOutboundPortURI = e.getKey();
			OutboundConnection connection = e.getValue();
			
			GossipOutboundPort gossipOutboundPort = new GossipOutboundPort(gossipOutboundPortURI, this.getOwner());
			connection.gossipOutboundPort = gossipOutboundPort;
			gossipOutboundPort.publishPort();
		}
	}
	
	/**
	 * Utility method for connecting each plugin outbound port to its inbound port counter part.
	 * Must be called in the {@code start} method of its Broker owner component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post {@code 
	 *   forall c in gossipOutboundPorts.values():
	 *       c.gossipOutboundPort.connected()
	 * }   // all outbound ports are connected.
	 * </pre>
	 * 
	 * @throws Exception If an error occurs while connecting an outbound port.
	 */
	public void connectPorts() throws Exception {
		// For each outbound connection, connect the outbound port
		for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
			OutboundConnection connection = e.getValue();
			this.getOwner().doPortConnection(connection.gossipOutboundPort.getPortURI(),
					connection.portToConnectURI,
					GossipServiceConnector.class.getCanonicalName());
			
			assert connection.gossipOutboundPort.connected() : 
				new PostconditionException(connection.gossipOutboundPort.getPortURI()+" not connected");
		}
	}
	
	@Override
	public void	finalise() throws Exception {
		// Each outbound port is disconnected
		for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
			OutboundConnection connection = e.getValue();
			this.getOwner().doPortDisconnection(connection.gossipOutboundPort.getPortURI());
		}
		super.finalise();
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
		super.uninstall();
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------
	
	/**
	 * Marks the given gossip messages as locally received (by putting them into {@code this.receivedMessages}) 
	 * and sends them to another(s) {@link Broker}'s {@link BrokerGossipInboundPort} with gossip protocol threads.<br>
	 * This method is only called by Broker services called by local clients (in the same Broker JVM).
	 * 
	 * <p>Messages will be sent to Brokers to which {@code gossipOutboundPort}s are currently connected.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	all messages in {@code gossipMessages} have been locally treated in the {@link Broker} owner. // informal
	 * pre	{@code gossipMessages != null && gossipMessages.length > 0}
	 * pre	{@code Stream.of(gossipMessages).allMatch(m -> m != null && m instanceof GossipMessage))}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param gossipMessages	gossip messages sent by the sender in the gossip protocol.
	 */
	public void send(GossipMessageI[] gossipMessages) {
		// Precondition check
		assert(gossipMessages != null && gossipMessages.length > 0) : new PreconditionException("gossipMessages cannot be null or empty");
		assert(Stream.of(gossipMessages).allMatch(m -> m != null && m instanceof GossipMessage)) : new PreconditionException("Must contain only non null GossipMessage");
		
		Broker broker = (Broker)this.getOwner();
		
		synchronized(this.receivedMessages) {
			for (int i=0; i<gossipMessages.length; i++) {
				GossipMessage gossipMessage = (GossipMessage)gossipMessages[i];
				this.receivedMessages.add(gossipMessage);
			}
		}
		
		
		for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
			broker.runTask(Broker.GOSSIP_POOL_URI, o -> {
				GossipOutboundPort gossipOutboundPort = e.getValue().gossipOutboundPort;
				try {
					gossipOutboundPort.send(gossipMessages);
				} catch (Exception eSend) {
					eSend.printStackTrace();
				}
			});
		}
	}
	
	/**
	 * Receives an array of gossip messages from another {@link Broker}.
	 * 
	 * <ul>
	 * 	<li>First sorts {@code gossipMessages} by keeping only gossip messages not yet locally received, and in the same 
	 * 		time marks every received gossip message as locally received from now, by adding them into {@code this.receivedMessages}.</li>
	 * 	<li>Then delegates to other gossip threads the propagation of received messages not previously received 
	 * 		(those kept in the 'keptGossipMessages' array).</li>
	 * 	<li>Finally, delegates {@link Broker} updates through a call to the {@code update} method.</li>
	 * <ul>
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
		
		List<GossipMessage> messagesToKeep = new ArrayList<>(); // must be instances of 'GossipMessage'
		
		synchronized(this.receivedMessages) {
			for (int i=0; i<gossipMessages.length; i++) {
				GossipMessage gossipMessage = (GossipMessage)gossipMessages[i];
				// if the message has not yet been locally received, keeps it for treatment
				if (!(this.receivedMessages.contains(gossipMessage))) messagesToKeep.add(gossipMessage);
				// marks the message as locally received
				this.receivedMessages.add(gossipMessage);
			}
		}
		
		int l = messagesToKeep.size();
		if (l > 0) {
			
			GossipMessageI[] keptGossipMessages = new GossipMessageI[l]; // List 'messagesToKeep' to Array
			for (int j=0; j<l; j++) {
				keptGossipMessages[j] = messagesToKeep.get(j);
			}
			
			GossipMessageI[] messagesToSendToNeighbours = new GossipMessageI[l]; // Copies gossip messages for propagation
			for (int j=0; j<l; j++) {
				messagesToSendToNeighbours[j] = keptGossipMessages[j].copyWithNewEmitterURI(broker.getReflectionInboundPortURI());
			}
			
			// Delegates propagation of messages previously not received to other gossip threads.
			for (Map.Entry<String, OutboundConnection> e : gossipOutboundPorts.entrySet()) {
				OutboundConnection connection = e.getValue();
					broker.runTask(Broker.GOSSIP_POOL_URI, o -> {
						GossipOutboundPort gossipOutboundPort = connection.gossipOutboundPort;
						try {
							gossipOutboundPort.send(messagesToSendToNeighbours);
						} catch (Exception eSend) {
							eSend.printStackTrace();
						}
					});

			}
			
			// Delegates Broker updates through a call to the update method
			update(keptGossipMessages);
		}
	}

	/**
	 * Considers the given gossip messages received from another sender {@link Broker}, the current thread will :
	 * 
	 * <ul>
	 * 	<li>Delegate the associated action of each gossip message {@code fromSender} to another gossip thread.</li>
	 * 	<li>Eventually clean old received messages in {@code this.receivedMessages}, if the number of messages exceeds a certain threshold
	 * 			and some messages exceed maximum storage time.</li>
	 * </ul>
	 * 
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code fromSender != null && fromSender.length > 0}
	 * pre	{@code Stream.of(fromSender).allMatch(m -> m != null && m instanceof GossipMessage)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param fromSender					gossip messages received from the sender in the gossip protocol.
	 * @throws IllegalArgumentException		if an action of a received message has no case in the local switch.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void update(GossipMessageI[] fromSender) {
		// Precondition check
		assert(fromSender != null && fromSender.length > 0) : new PreconditionException("fromSender cannot be null or empty");
		assert(Stream.of(fromSender).allMatch(m -> m != null && m instanceof GossipMessage)) : new PreconditionException("Must contain only non null GossipMessage");
			
		Broker broker = (Broker)this.getOwner();
		
		// Delegate the associated action of each gossip message to another gossip thread
		for (int i=0; i<fromSender.length; i++) {
			GossipMessage gossipMessage = (GossipMessage)fromSender[i];
			MessageI parameters = gossipMessage.message();
			
			broker.runTask(Broker.GOSSIP_POOL_URI, o -> {
				try {
					switch (gossipMessage.messageAction()) {
						case CREATE_CHANNEL:
							broker.createChannelService((String)parameters.getPropertyValue(GossipMessage.ARGS_CREATE_CHANNEL.get(0)), 
									(String)parameters.getPropertyValue(GossipMessage.ARGS_CREATE_CHANNEL.get(1)),
									(String)parameters.getPropertyValue(GossipMessage.ARGS_CREATE_CHANNEL.get(2)),
									false);
							break;
						case DESTROY_CHANNEL:
							boolean now = (boolean)parameters.getPropertyValue(GossipMessage.ARGS_DESTROY_CHANNEL.get(2));
							if (now) {
								broker.destroyChannelNowService((String)parameters.getPropertyValue(GossipMessage.ARGS_DESTROY_CHANNEL.get(0)), 
										(String)parameters.getPropertyValue(GossipMessage.ARGS_DESTROY_CHANNEL.get(1)),
										false);
							} else {
								broker.destroyChannelService((String)parameters.getPropertyValue(GossipMessage.ARGS_DESTROY_CHANNEL.get(0)), 
										(String)parameters.getPropertyValue(GossipMessage.ARGS_DESTROY_CHANNEL.get(1)),
										false);
							}
							break;
						case PUBLISH:
							broker.publishService((String)parameters.getPropertyValue(GossipMessage.ARGS_PUBLISH.get(0)), 
									(String)parameters.getPropertyValue(GossipMessage.ARGS_PUBLISH.get(1)),
									(ArrayList<MessageI>)parameters.getPropertyValue(GossipMessage.ARGS_PUBLISH.get(2)),
									false);
							break;
						case REGISTER_CLIENT:
							broker.registerService((String)parameters.getPropertyValue(GossipMessage.ARGS_REGISTER_CLIENT.get(0)), 
									(RegistrationClass)parameters.getPropertyValue(GossipMessage.ARGS_REGISTER_CLIENT.get(1)),
									false);
							break;
						case SUBSCRIBE_CLIENT_CHANNEL:
							broker.subscribeService((String)parameters.getPropertyValue(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(0)), 
									(String)parameters.getPropertyValue(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(1)),
									(MessageFilterI)parameters.getPropertyValue(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(2)),
									false);
							break;
						case UNREGISTER_CLIENT:
							broker.unregisterService((String)parameters.getPropertyValue(GossipMessage.ARGS_UNREGISTER_CLIENT.get(0)), 
									false);
							break;
						case UNSUBSCRIBE_CLIENT_CHANNEL:
							broker.unsubscribeService((String)parameters.getPropertyValue(GossipMessage.ARGS_UNSUBSCRIBE_CLIENT_CHANNEL.get(0)), 
									(String)parameters.getPropertyValue(GossipMessage.ARGS_UNSUBSCRIBE_CLIENT_CHANNEL.get(1)),
									false);
							break;
						case UPDATE_CHANNEL_AUTHORIZED_URIS:
							broker.modifyAuthorisedUsersService((String)parameters.getPropertyValue(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(0)), 
									(String)parameters.getPropertyValue(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(1)),
									(String)parameters.getPropertyValue(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(2)),
									false);
							break;
						case UPDATE_CLIENT_SERVICE_CLASS:
							broker.modifyServiceClassService((String)parameters.getPropertyValue(GossipMessage.ARGS_UPDATE_CLIENT_SERVICE_CLASS.get(0)), 
									(RegistrationClass)parameters.getPropertyValue(GossipMessage.ARGS_UPDATE_CLIENT_SERVICE_CLASS.get(1)),
									false);
							break;
						case NOTHING:
							break;
						default:
							throw new IllegalArgumentException("Undefined received message action in gossip plugin switch, broker"+broker.id);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}
		
		synchronized(this.receivedMessages) {
			// Eventually clean old received messages
			if (this.receivedMessages.size() > GOSSIP_MESSAGES_THRESHOLD) {
				Instant iNow = Instant.now();
				this.receivedMessages.removeIf(msg -> {
					Duration duration = Duration.between(msg.timestamp(), iNow);
					return duration.toMillis() > MAX_SECONDS_DIFF*1000;
				});
			}
		}
		
	}

}
