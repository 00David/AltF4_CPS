package fr.sorbonne_u.broker.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.apache.commons.math3.random.RandomDataGenerator;

import fr.sorbonne_u.broker.component.tasks.LoaderTask;
import fr.sorbonne_u.broker.component.tasks.SenderTask;
import fr.sorbonne_u.broker.component.tasks.UnloaderTask;
import fr.sorbonne_u.broker.connectors.ReceivingServiceConnector;
import fr.sorbonne_u.broker.plugin.BrokerGossipPlugin;
import fr.sorbonne_u.broker.ports.ReceivingOutboundPort;
import fr.sorbonne_u.broker.ports.BrokerPrivilegedInboundPort;
import fr.sorbonne_u.broker.ports.BrokerPublishingInboundPort;
import fr.sorbonne_u.broker.ports.BrokerRegistrationInboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.tasks_management.connection.AbnormalTerminationNotificationOutboundPort;
import fr.sorbonne_u.components.tasks_management.interfaces.AbnormalTerminationNotificationCI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipReceiverCI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.gossip.GossipMessage;
import fr.sorbonne_u.messages.gossip.GossipMessageFactory;
import fr.sorbonne_u.meteo.data.PositionData;

/**
 * Component implementing the publish/subscribe system.
 * 
 * <p><strong>About the publication service : </strong></p>
 * <ul>
 *   <li>It is firstly handled by a thread from the classic thread pool : it mostly verifies service preconditions.</li>
 *   <li>Secondly it is handled by a thread from one of the loading thread pools : it loads messages into a channel. 
 *   		Corresponding task : {@link LoaderTask}</li>
 *   <li>Thirdly it is handled by a thread from the unloading thread pool : is unloads messages from a channel. 
 *    		Corresponding task : {@link UnloaderTask}</li>
 *   <li>Finally, it is handled by a thread from the sending thread pool : it sends messages previously unloaded. 
 *   		Corresponding task : {@link SenderTask}</li>
 * </ul>
 * 
 * <p><strong>Main features : </strong></p>
 * <ul>
 *   <li>Manages client component registrations (via the URI of their inbound port implementing {@link ReceivingCI}).</li>
 *   <li>Manages transmission channels between these registered client components.</li>
 *   <li>Handles message transmissions by applying filters provided by components subscribed to the relevant channel.</li>
 *   <li>Handles privileges for non-FREE clients, with better performances on publication and advanced channel management.
 *   <p> More precisely <strong>about performances</strong>, there are differences on 2 layers : 
 *   	<ul>
 *			<li>Depending on the registration class of the PUBLISHING CLIENT, a different loading thread pool is used. 
 *				Higher-privileged registration classes have larger loading thread pools (scaled on the size of the FREE loading pool).</li>
 *			<li>Depending on the registration class of the CREATOR OF A CHANNEL, messages sent from it can be grouped. 
 *				Higher-privileged channel creator registration classes allows biggest groups of messages sent together 
 *				(if the creator is the Broker, or he is not registered anymore, it is only 1 message per sending).</li>
 *   	</ul>
 *   </li>
 *   <li>In a distributed deployment configuration, some services use the Gossip protocol.
 *   	Their last argument is a boolean flag indicating whether the service call originates from the same JVM.<br>
 *   	If the flag is true, once the call is completed, it is propagated to other broker neighbors through the {@link BrokerGossipPlugin}.
 *   </li>
 * </ul>
 * 
 * <p>Each provided service is multi-thread safe. <strong>About multi-thread handling : </strong></p>
 * <ul>
 *  <li>
 * 		2 {@link ConcurrentHashMap} store our client and channel informations : {@link Broker#clientsInfos} and {@link Broker#channelsInfos}.<br>
 * 		This allow us to perform simple single operations, like just a get/put, without having to take any lock.
 * 	</li>
 * 	<li>
 * 		On client registration and channel creation, specific locks are taken for ensuring that there are not 2 parallel threads
 * 		registering a client with the same port URI, or creating a channel with the same name.
 * 		(respectively with {@link Broker#newClientLock} and {@link Broker#newChannelLock})
 * 	</li>
 * 	<li>
 * 		For each service needing to access to a client informations : it will firstly get its {@link ClientBrokerInfos} instance
 * 		by calling the {@link Broker#getRegisteredClient(String)}, ensuring that it gets a non null reference of the registered client.<br>
 * 		Then it will wait for the object lock, and once it has it, it will verify that the client is still registered, and from that
 * 		step IT IS SURE that it is the only thread accessing to this client informations. While having the lock, it will perform
 * 		different operations with the client informations.
 * 	</li>
 * 	<li>
 * 		For each service needing to access to a channel informations EXCEPT FOR DESTROYING a channel : on the opposite of a client, 
 * 		a channel is not manipulated using a global synchronized lock on each instance. Instead operations on a channel are in general
 * 		delegated to the manipulated {@link ChannelBrokerInfos}, where the called method might use personalized locks for each
 * 		internal data. Thus, exceptions are also in general thrown from those called methods, just like postconditions on the manipulated
 * 		channel are also ensured within those called methods. 
 * 	</li>
 * 	<li>
 * 		For the special case of destroying a channel, the {@link ChannelBrokerInfos} instance lock is taken the same way as for the clients,
 * 		ensuring that a channel can only be simultaneously destroyed by one thread.<br>
 * 		Not manipulating the object lock for each channel informations, mostly for performance reasons, implies that while a channel 
 * 		is being manipulated by a thread A, this channel can be in parallel 'destroyed' (= JUST removed from the channels map, not put 
 * 		at null) by a thread B. Then A modifications will be reflected into the channel, but from the moment when this channel is destroyed, 
 * 		{@link LoaderTask}s and {@link UnloaderTask}s will interrupt loading/unloading messages from those channels.<br>
 * 		So even if it is modified while being destroyed, those modifications will not be taken into account because it will not 
 * 		be possible anymore to load/unload messages from it.
 * 	</li>
 * 	<li>
 * 		Finally, there is a forced order between client and channel locks : always {@link ClientBrokerInfos}, then {@link ChannelBrokerInfos}.
 * 	</li>
 * </ul>
 * 
 * <p><strong>About the gossip protocol : </strong></p>
 * <ul>
 * 	<li>
 * 		Each Broker is created by providing him its inbound and outbound gossip port URIs, as well as the port URIs corresponding
 * 		to gossip inbound ports of other Brokers. Each outbound port will connect to an inbound port of another Broker.
 * 	</li>
 * 	<li>
 * 		At the end of each service needing to spread the operation to other Brokers, so when the service has been called by a local
 * 		client (from the same Broker JVM), a {@link GossipMessage} is created and sent to the 
 * 		{@link BrokerGossipPlugin#storeForSending(GossipMessage[])} gossip plugin method.<br>
 * 	</li>
 * </ul>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code gossipPlugin != null && gossipPlugin.isInitialised()}
 * invariant	{@code GOSSIP_POOL_URI != null && !GOSSIP_POOL_URI.isEmpty() && nbGossipThreads > 0}
 * 
 * invariant	{@code notificationPort != null}
 * invariant	{@code registrationPortURI != null && !registrationPortURI.isEmpty()}
 * invariant	{@code registrationPort != null}
 * invariant	{@code publishingPortURI != null && !publishingPortURI.isEmpty()}
 * invariant	{@code publishingPort != null}
 * invariant	{@code privilegedPortURI != null && !privilegedPortURI.isEmpty()}
 * invariant	{@code privilegedPort != null}
 * 
 * invariant	{@code clientsInfos != null}
 * invariant	{@code channelsInfos != null}
 * 
 * invariant	{@code FREE_LOADERS_POOL_URI != null && !FREE_LOADERS_POOL_URI.isEmpty() && nbFreeLoadersThreads > 0}
 * invariant	{@code STD_LOADERS_POOL_URI != null && !STD_LOADERS_POOL_URI.isEmpty() && nbStdLoadersThreads > 0}
 * invariant	{@code PREM_LOADERS_POOL_URI != null && !notificationPortPREM_LOADERS_POOL_URI.isEmpty() && nbPremLoadersThreads > 0}
 * invariant	{@code UNLOADERS_POOL_URI != null && !UNLOADERS_POOL_URI.isEmpty() && nbUnloadersThreads > 0}
 * invariant	{@code SENDERS_POOL_URI != null && !SENDERS_POOL_URI.isEmpty() && nbSendersThreads > 0}
 * 
 * invariant	{@code newClientLock != null}
 * invariant	{@code newChannelLock != null}
 * invariant 	{@code synchronized(getRegisteredClient(receptionPortURI)){...}} ensures that AT MOST 1 THREAD has this client lock
 * invariant 	{@code synchronized(getExistingChannel(channelName)){...}} ensures that AT MOST 1 THREAD has this channel lock
 * invariant	FORCED LOCK ORDER : synchronized({@link ClientBrokerInfos}) -> synchronized({@link ChannelBrokerInfos}) (for any of those instances)
 * </pre>
 * 
 * @see BrokerGossipPlugin for more informations on the gossip protocol implementation.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
@OfferedInterfaces(offered = {RegistrationCI.class, PublishingCI.class, PrivilegedClientCI.class})
@RequiredInterfaces(required = {ReceivingCI.class, AbnormalTerminationNotificationCI.class})
public class Broker extends AbstractComponent{
	
	/** Broker id given in the constructor, for debugging purpose in a distributed environment. */
	public final int id;
	
	//--------------------------------------------------------------------------
	// Internal datas
	//--------------------------------------------------------------------------
	
	// GOSSIP PROTOCOL
	
	/** Broker's gossip protocol plugin, every Broker has it initialized in its constructor. */
	private final BrokerGossipPlugin gossipPlugin;
	
	/** URI of the pool of threads used for the gossip protocol message sending. */
	public static final String	GOSSIP_SENDING_POOL_URI = "gossip-sending-pool" ;
	
	/** URI of the pool of threads used for the gossip protocol message reception. */
	public static final String	GOSSIP_RECEIVING_POOL_URI = "gossip-receiving-pool" ;
	
	/** Number of threads in both {@code GOSSIP_SENDING_POOL_URI} and {@code GOSSIP_RECEIVING_POOL_URI} pools. */
	public final int nbGossipThreads;
	
	// NOTIFICATION
	
	/** Broker's outbound port, implementing {@link AbnormalTerminationNotificationCI} */
	private final AbnormalTerminationNotificationOutboundPort notificationPort;
	
	// REGISTRATION
	
	/** 
	 * List of available Broker inbound registration ports. One of it is got by the {@link Broker#registrationPortURI()} method.
	 * <p>Each URI is for a different Broker, and each access to it must be done by using the list synchronized monitor.</p> 
	 */
	private static List<String> registrationPortURIs = new ArrayList<>(); // In our tests, we only have one Broker per JVM, but it is possible to have more with this
	
	/** Random generator, used for choosing a Broker registration URI in {@code registrationPortURIs}. */
	private static final RandomDataGenerator random = new RandomDataGenerator();
	
	/**
	 * Public getter for allowing clients to get the broker registration URI.
	 * 
	 * <p>Select a random Broker registration port URI if multiples are available<p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code !registrationPortURIs.isEmpty()}
	 * post class invariants satisfied
	 * </pre>
	 * 
	 * @return The URI of the choosen {@link BrokerRegistrationInboundPort}.
	 */
	public static String registrationPortURI() {
		synchronized(Broker.registrationPortURIs) {
			// Precondition check
			assert (!registrationPortURIs.isEmpty()) : 
				new PreconditionException("No registration ports available");
		    
		    int index = random.nextInt(0, registrationPortURIs.size() - 1);
		    String registrationPortURI = registrationPortURIs.get(index);
		    
		    // Postcondition check
		    assert (registrationPortURI != null && ! registrationPortURI.isEmpty()):
		    	new PostconditionException("registrationPortURI cannot be null or empty");
		    return registrationPortURI;
		}
	}
	
	/** Broker's inbound port, implementing {@link RegistrationCI} */
	private final BrokerRegistrationInboundPort registrationPort;
	
	// PUBLICATION
	
	/** Broker's inbound publication port, got by registering an inbound client port */
	private String publishingPortURI;
	
	/** Broker's inbound port, implementing {@link PublishingCI} */
	private final BrokerPublishingInboundPort publishingPort;
	
	// PRIVILEGED MANAGEMENT
	
	/** Broker's inbound privileged port, got by registering an inbound privileged client port */
	private String privilegedPortURI;
	
	/** Broker's inbound port, implementing {@link PrivilegedClientCI} */
	private final BrokerPrivilegedInboundPort privilegedPort;

	// DATA MAPPINGS
	
	/** 
	 * Mapping : client inbound port (URI) -> its informations.
	 */
	private Map<String, ClientBrokerInfos> clientsInfos = new ConcurrentHashMap<>(); 
	
	/** 
	 * Mapping : channel name -> its informations.
	 */
	private Map<String, ChannelBrokerInfos> channelsInfos = new ConcurrentHashMap<>();
	
	// CONCURRENT PUBLISHING PERFORMANCES
	
	// For loading messages into channels + creating unloading tasks. 
	// A different pool according to the registration class of the publisher.
	
	/** URI of the pool of threads used to load messages into a channel, coming from a FREE client. */
	public static final String	FREE_LOADERS_POOL_URI = "free-loaders-pool" ;
	
	/** Number of threads in the {@code FREE_LOADERS_POOL_URI} pool. */
	public final int nbFreeLoadersThreads;
	
	/** URI of the pool of threads used to load messages into a channel, coming from a STANDARD client. */
	public static final String	STD_LOADERS_POOL_URI = "std-loaders-pool" ;
	
	/** Number of threads in the {@code STD_LOADERS_POOL_URI} pool. 2 x {@code nbFreeLoadersThreads} */
	public final int nbStdLoadersThreads;
	
	/** URI of the pool of threads used to load messages into a channel, coming from a PREMIUM client. */
	public static final String	PREM_LOADERS_POOL_URI = "prem-loaders-pool" ;
	
	/** Number of threads in the {@code PREM_LOADERS_POOL_URI} pool. 4 x {@code nbFreeLoadersThreads} */
	public final int nbPremLoadersThreads;
	
	// For unloading messages from channels + creating sending tasks.
	
	/** URI of the pool of threads used to unload messages from channels. */
	public static final String	UNLOADERS_POOL_URI = "unloaders-pool" ;
	
	/** Number of threads in the {@code UNLOADERS_POOL_URI} pool. */
	public final int nbUnloadersThreads;
	
	// For sending unloaded messages to a targeted client.
	
	/** URI of the pool of threads used to send messages. */
	public static final String	SENDERS_POOL_URI = "senders-pool" ;
	
	/** Number of threads in the {@code SENDERS_POOL_URI} pool. */
	public final int nbSendersThreads;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	/**
	 * Creates a Broker with 'N' channels.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code N >= 2}
	 * pre	{@code nbThreads > 0}
	 * pre	{@code nbSchedulableThreads >= 0}
	 * pre	{@code nbFreeLoadersThreads > 0 && nbUnloadersThreads > 0 && nbSendersThreads > 0}
	 * pre	{@code nbGossipThreads > 0}
	 * post class invariants satisfied
	 * post outbound ports configured and initialized
	 * post {@code ClientBrokerInfos.nbMaxChannelsStd >= 0 && ClientBrokerInfos.nbMaxChannelsPrem >= 0}
	 * </pre>
	 * 
	 * @param id						Broker id. Supposed to be unique, even in a multi-JVM run situation.
	 * @param windowPosition			Broker's log window position. A PositionData is used for it.
	 * @param N							The number of default channels in the Broker (from 'channel0' to 'channel[N-1]').
	 * @param nbMaxChannelsStd			The maximum number of channels that can be managed by a standard privileged client.
	 * @param nbMaxChannelsPrem			The maximum number of channels that can be managed by a premium privileged client.
	 * @param nbThreads					Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		Number of threads to be created in the component schedulable pool.
	 * @param nbFreeLoadersThreads		Number of threads to be created in the free loaders pool. It is x2 for standard clients, and x4 for premium clients.
	 * @param nbUnloadersThreads		Number of threads to be created in the unloaders pool.
	 * @param nbSendersThreads			Number of threads to be created in the senders pool.
	 * @param nbGossipThreads			Number of threads to be created in both sending and receiving gossip protocol pools.
	 * @param gossipInboundPortURI		The {@link Broker} inbound port URI offering {@link GossipReceiverCI} interface.
	 * @param gossipOutboundPortURIs	{@link Broker} outbound port URIs requiring {@link GossipSenderCI} interface.
	 * @param otherBrokersToConnectURIs	Other {@link Broker}s gossip inbound port URIs, a URI at an index i in {@code gossipOutboundPortURIs} 
	 * 									connects to the port URI with the same index in {@code otherBrokersToConnectURIs}.
	 * @throws Exception				If there is a problem while publishing a port.
	 */
	protected Broker(int id, PositionData windowPosition, int N, int nbMaxChannelsStd, int nbMaxChannelsPrem, int nbThreads, int nbSchedulableThreads,
			int nbFreeLoadersThreads, int nbUnloadersThreads, int nbSendersThreads, int nbGossipThreads,
			String gossipInboundPortURI, List<String> gossipOutboundPortURIs, List<String> otherBrokersToConnectURIs) throws Exception {
		super(nbThreads, nbSchedulableThreads);
		// Precondition checks
		assert(windowPosition != null) : 
			new PreconditionException("windowPosition cannot be null");
		assert(N>=2) : 
			new PreconditionException("Broker's number of default channels must be at least 2 ('channel0' for wind datas, 'channel1' for meteo alerts)");
		assert(nbThreads > 0) : 
			new PreconditionException("must have at leat 1 normal thread");
		assert(nbSchedulableThreads >= 0) : 
			new PreconditionException("cannot have a negative number of schedulable threads");
		assert(nbFreeLoadersThreads > 0 && nbUnloadersThreads > 0 && nbSendersThreads > 0) : 
			new PreconditionException("must have at least 1 thread for each publication step");
		assert(nbGossipThreads > 0) : 
			new PreconditionException("must have at leat 1 gossip thread");
		
		this.id = id;
		
		// Port URIs
		String registrationPortURI = "registration-inbound-port-broker"+id;
		synchronized(Broker.registrationPortURIs) {
			registrationPortURIs.add(registrationPortURI);
		}
		publishingPortURI = "publishing-inbound-port-broker"+id;
		privilegedPortURI = "privileged-inbound-port-broker"+id;
		
		// ClientBrokerInfos internal static data initializations
		ClientBrokerInfos.nbMaxChannelsStd = nbMaxChannelsStd;
		ClientBrokerInfos.nbMaxChannelsPrem = nbMaxChannelsPrem;
		
		// Creation of N channels
		// The channel0 is for wind datas, and the channel1 for meteo alerts
		for (int i = 0; i < N; i++) {
			String chan = "channel"+i;		
			channelsInfos.put(chan, new ChannelBrokerInfos(chan, ".*", null));
		}
		
		// Creation of thread pools
		this.nbGossipThreads = nbGossipThreads;
		this.nbFreeLoadersThreads = nbFreeLoadersThreads;
		this.nbStdLoadersThreads = 2*nbFreeLoadersThreads;
		this.nbPremLoadersThreads = 4*nbFreeLoadersThreads;
		this.nbUnloadersThreads = nbUnloadersThreads;
		this.nbSendersThreads = nbSendersThreads;
		
		this.createNewExecutorService(GOSSIP_SENDING_POOL_URI, this.nbGossipThreads, true);
		this.createNewExecutorService(GOSSIP_RECEIVING_POOL_URI, this.nbGossipThreads, false);
		this.createNewExecutorService(FREE_LOADERS_POOL_URI, this.nbFreeLoadersThreads, false);
		this.createNewExecutorService(STD_LOADERS_POOL_URI, this.nbStdLoadersThreads, false);
		this.createNewExecutorService(PREM_LOADERS_POOL_URI, this.nbPremLoadersThreads, false);
		this.createNewExecutorService(UNLOADERS_POOL_URI, this.nbUnloadersThreads, false);
		this.createNewExecutorService(SENDERS_POOL_URI, this.nbSendersThreads, false);

		// Ports creation and publication
		this.notificationPort = new AbnormalTerminationNotificationOutboundPort(this);
		this.notificationPort.publishPort();
		
		this.registrationPort = new BrokerRegistrationInboundPort(registrationPortURI, this);
		this.registrationPort.publishPort();
		
		this.publishingPort = new BrokerPublishingInboundPort(publishingPortURI, this, this.notificationPort);
		this.publishingPort.publishPort();
		
		this.privilegedPort = new BrokerPrivilegedInboundPort(privilegedPortURI, this, this.notificationPort);
		this.privilegedPort.publishPort();
		
		// Install the gossip plug-in.
		this.gossipPlugin = new BrokerGossipPlugin(gossipInboundPortURI, gossipOutboundPortURIs, otherBrokersToConnectURIs);
		this.gossipPlugin.setPluginURI("gossip");
		this.installPlugin(this.gossipPlugin);
		
		// Postcondition checks
		assert	this.isRequiredInterface(GossipSenderCI.class) :
			new InvariantException("The Broker component should require the interface GossipSenderCI!");
		assert	this.isOfferedInterface(GossipReceiverCI.class) :
			new InvariantException("The Broker component should offer the interface GossipReceiverCI!");
		assert	this.isRequiredInterface(AbnormalTerminationNotificationCI.class) :
			new InvariantException("The Broker component should require the interface AbnormalTerminationNotificationCI!");
		assert	this.isOfferedInterface(RegistrationCI.class) :
			new InvariantException("The Broker component should offer the interface RegistrationCI!");
		assert	this.isOfferedInterface(PublishingCI.class) :
			new InvariantException("The Broker component should offer the interface PublishingCI!");
		assert	this.isOfferedInterface(PrivilegedClientCI.class) :
			new InvariantException("The Broker component should offer the interface PrivilegedClientCI!");
		assert	this.isRequiredInterface(ReceivingCI.class) :
			new InvariantException("The Broker component should require the interface ReceivingCI!");
		
		assert gossipPlugin.isInitialised() : 
			new PostconditionException("Gossip plugin must be initialized");
		assert	this.isPortExisting(gossipInboundPortURI) : 
			new PostconditionException("The component must have a port with URI " + gossipInboundPortURI);
		assert	this.findPortFromURI(gossipInboundPortURI).getImplementedInterface().equals(GossipReceiverCI.class) : 
			new PostconditionException("The component must have a port with implemented interface GossipReceiverCI");
		assert	this.findPortFromURI(gossipInboundPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + gossipInboundPortURI);
		for (String gossipOutboundPortURI : gossipOutboundPortURIs) {
			assert	this.isPortExisting(gossipOutboundPortURI) : 
				new PostconditionException("The component must have a port with URI " + gossipOutboundPortURI);
			assert	this.findPortFromURI(gossipOutboundPortURI).getImplementedInterface().equals(GossipSenderCI.class) : 
				new PostconditionException("The component must have a port with implemented interface GossipSenderCI");
			assert	this.findPortFromURI(gossipOutboundPortURI).isPublished() :
				new PostconditionException("The component must have a port published with URI " + gossipOutboundPortURI);
		}
		
		String[] notificationURIs = this.findOutboundPortURIsFromInterface(AbnormalTerminationNotificationCI.class);
		assert  notificationURIs.length == 1 : 
			new PostconditionException("The Broker must have a port with implemented interface AbnormalTerminationNotificationCI");;
		assert  this.findPortFromURI(notificationURIs[0]).isPublished() :
			new PostconditionException("The Broker must have a notification port published");
		
		assert	this.isPortExisting(registrationPortURI) : 
			new PostconditionException("The component must have a port with URI " + registrationPortURI);
		assert	this.findPortFromURI(registrationPortURI).getImplementedInterface().equals(RegistrationCI.class) : 
			new PostconditionException("The component must have a port with implemented interface RegistrationCI");
		assert	this.findPortFromURI(registrationPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + registrationPortURI);
		
		assert	this.isPortExisting(publishingPortURI) :
			new PostconditionException("The component must have a port with URI " + publishingPortURI);
		assert	this.findPortFromURI(publishingPortURI).getImplementedInterface().equals(PublishingCI.class) :
			new PostconditionException("The component must have a port with implemented interface RegistrationCI");
		assert	this.findPortFromURI(publishingPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + publishingPortURI);
		
		assert	this.isPortExisting(privilegedPortURI) :
			new PostconditionException("The component must have a port with URI " + privilegedPortURI);
		assert	this.findPortFromURI(privilegedPortURI).getImplementedInterface().equals(PrivilegedClientCI.class) :
			new PostconditionException("The component must have a port with implemented interface PrivilegedClientCI");
		assert	this.findPortFromURI(privilegedPortURI).isPublished() : 
			new PostconditionException("The component must have a port published with URI " + privilegedPortURI);
		
		
		assert ClientBrokerInfos.nbMaxChannelsStd >= 0 : 
			new PostconditionException("The max number of channels for standard clients must be properly initialized");
		assert ClientBrokerInfos.nbMaxChannelsPrem >= 0 : 
			new PostconditionException("The max number of channels for premium clients must be properly initialized");
		
		this.getTracer().setTitle("Broker"+id);
		this.getTracer().setRelativePosition((int)windowPosition.getPosX(), (int)windowPosition.getPosY());
	}
	
	//--------------------------------------------------------------------------
	// Component life-cycle
	//--------------------------------------------------------------------------

	@Override
	public synchronized void start() throws ComponentStartException {
		super.start();
		try {
			this.gossipPlugin.connectPorts(); // Connect gossip outbound ports
		} catch (Exception e) {
			throw new ComponentStartException(e) ;
		}
		this.logMessage("starting Broker component.");
	}

	@Override
	public synchronized void finalise() throws Exception {
		this.logMessage("stopping Broker component.");
		
		// Disconnect all outbound ports linked to registered clients
		for (Map.Entry<String, ClientBrokerInfos> entry : clientsInfos.entrySet()) {
			entry.getValue().disconnectClientPort(this);
		}
		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		
		try {
			// Unpublish all outbound ports linked to registered clients
			for (Map.Entry<String, ClientBrokerInfos> entry : clientsInfos.entrySet()) {
				entry.getValue().unpublishClientPort();
			}
		
			// Unpublish Broker notification outbound port + all inbound ports
			this.notificationPort.unpublishPort();
			this.registrationPort.unpublishPort();
			this.publishingPort.unpublishPort();
			this.privilegedPort.unpublishPort();
		} catch (Throwable e) {
			throw new ComponentShutdownException(e);
		}
		
		super.shutdown();
	}
	
	//--------------------------------------------------------------------------
	// Broker concurrency management
	//--------------------------------------------------------------------------
	
	/** 
	 * Lock used on a new client registration : 
	 * in case of 2 simultaneous calls for registering a client with the same port URI, one of those calls will throw an exception.
	 * @see Broker#registerService(String, RegistrationClass, boolean)
	 */
	private final ReentrantLock newClientLock = new ReentrantLock(true); // fair
	
	/** 
	 * Lock used on a new channel creation : 
	 * in case of 2 simultaneous calls for creating a channel with the same name, one of those calls will throw an exception.
	 * @see Broker#createChannelService(String, String, String, boolean)
	 */
	private final ReentrantLock newChannelLock = new ReentrantLock(true); // fair
	
	/**
	 * Returns the {@link ClientBrokerInfos} associated to the given {@code receptionPortURI}.
	 * 
	 * <p>
	 * This method either returns a non null {@link ClientBrokerInfos}, or throws a {@link UnknownClientException},
	 * this has a double utility :
	 * </p>
	 * <ul>
	 * 	<li>It does the user registered verification.</li>
	 * 	<li>It ensures that if no exception is thrown, the associated object is non null : then services can
	 * 		synchronized(...) on it with no more verification.</li>
	 * 	<li>Otherwise services would have to separately : 
	 * 		<ul>
	 * 			<li>1. Call registeredService method to see if the client is registered.</li>
	 * 			<li>2. Get the associated {@link ClientBrokerInfos} from the map, with no insurance on having a non null object.
	 * 				(the client could be unregistered and removed from the map between the previous call and this action)
	 * 			</li>
	 * 			<li>3. synchronized() on the object got, with potentially a NullPointerException if no verification.</li>
	 * 		</ul>
	 * 	</li>
	 * </ul>
	 * 
	 * <p>
	 * EXCEPT FOR A SINGLE ISOLATED OPERATION ON A {@code this.clientsInfos} MAP VALUE, 
	 * EACH SERVICE MUST GET CLIENT INFORMATIONS VIA THIS METHOD.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @return								A non null object, containing {@code receptionPortURI} client informations in the system.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 */
	private ClientBrokerInfos getRegisteredClient(String receptionPortURI)
			throws UnknownClientException{
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		if (client == null) 
			throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		
		return client;
	}
	
	/**
	 * Returns the {@link ChannelBrokerInfos} associated to the given {@code channelName}.
	 * 
	 * <p>This method either returns a non null {@link ChannelBrokerInfos}, or throws a {@link UnknownChannelException} :
	 * this has a double utility :
	 * </p>
	 * <ul>
	 * 	<li>It does the existing channel verification.</li>
	 * 	<li>It ensures that if no exception is thrown, the associated object is non null : then services can
	 * 		synchronized(...) on it with no more verification.</li>
	 * 	<li>Otherwise services would have to separately : 
	 * 		<ul>
	 * 			<li>1. Call channelExistService method to see if the channel exists.</li>
	 * 			<li>2. Get the associated {@link ChannelBrokerInfos} from the map, with no insurance on having a non null object.
	 * 				(the channel could have been destroyed and removed from the map between the previous call and this action)
	 * 			</li>
	 * 			<li>3. synchronized() on the object got, with potentially a NullPointerException if no verification.</li>
	 * 		</ul>
	 * 	</li>
	 * </ul>
	 * 
	 * <p>
	 * EXCEPT FOR A SINGLE ISOLATED OPERATION ON A {@code channelsInfos} MAP VALUE, 
	 * EACH SERVICE MUST GET CHANNEL INFORMATIONS VIA THIS METHOD.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channelName != null && !channelName.isEmpty()}
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * @param channelName					Name of a potential channel.
	 * @return								A non null object, containing {@code channelName} channel informations in the system.
	 * @throws UnknownChannelException      If the {@code channelName} channel does not exist.
	 */
	private ChannelBrokerInfos getExistingChannel(String channelName)
			throws UnknownChannelException{
		// Precondition checks
		assert(channelName != null && !channelName.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		
		ChannelBrokerInfos channel = channelsInfos.get(channelName);
		if (channel == null) 
			throw new UnknownChannelException("Channel "+channel+" does not exist.");
		
		return channel;
	}
	
	//--------------------------------------------------------------------------
	// Component internal services
	//--------------------------------------------------------------------------
	
	// REGISTRATIONS

	/**
	 * Return true if {@code receptionPortURI} has already been registered, otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @return					true if {@code receptionPortURI} has already been registered, otherwise false.
	 */
	public boolean registeredService(String receptionPortURI){
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		
		return clientsInfos.containsKey(receptionPortURI);
	}
	
	/**
	 * Return true if {@code receptionPortURI} has already been registered with service class {@code rc}, otherwise false.<br>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	           	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				           	the registration class.
	 * @return					           	true if {@code receptionPortURI} has already been registered, 
	 * 										with service class {@code rc} if given, otherwise false.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 */
	public boolean registeredService(String receptionPortURI, RegistrationClass rc) 
			throws UnknownClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(rc != null) : 
			new PreconditionException("registration class cannot be null");
		
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		return client.getRegistrationClass().equals(rc);
	}

	/**
	 * Register on the publication/subscription system passing the URI of the inbound port offering the component 
	 * interface {@code ReceivingCI}.<br>
	 * Returns an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * 
	 * <p>A client registration lock is used to ensure not having 2 same client port URI registrations simultaneously.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code registeredService(receptionPortURI)}
	 * post	{@code registeredService(receptionPortURI, rc)}
	 * </pre>
	 *
	 * @param receptionPortURI	          	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				          	the required registration class.
	 * @param fromLocalJVM					True if the call comes from a client in the same Broker JVM, and thus need to 
	 * 										be propagated with the gossip protocol.
	 * @return					          	an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * @throws AlreadyRegisteredException 	If the client has already been registered.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 */
	public String registerService(String receptionPortURI, RegistrationClass rc, boolean fromLocalJVM) 
			throws AlreadyRegisteredException, UnknownClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(rc != null) : 
			new PreconditionException("registration class cannot be null");
		
		newClientLock.lock();
		try {
			if (registeredService(receptionPortURI)) throw new AlreadyRegisteredException("Client "+receptionPortURI+" already registered.");
			
			this.logMessage("Thread "+Thread.currentThread().getId()+": Broker registering '" + receptionPortURI + "'") ;
			
			ReceivingOutboundPort outboundPort = null;
			// Create the Broker outbound port for sending messages to the receptionPortURI
			// New outbound port published + connected to the client receptionPortURI
			// If the call does not come from the same JVM, but via the gossip protocol, then there is no outboundPort associated to the client.
			if (fromLocalJVM) {
				try {
					outboundPort = new ReceivingOutboundPort("BrokerOutboundPort-"+receptionPortURI, this);
					outboundPort.publishPort() ;
					this.doPortConnection(outboundPort.getPortURI(), receptionPortURI, ReceivingServiceConnector.class.getCanonicalName());
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
			// Add the receptionPortURI client infos to the mapping of the client infos
			// Takes the client lock
			ClientBrokerInfos client = new ClientBrokerInfos(receptionPortURI, rc, outboundPort);
			synchronized(client) {
				clientsInfos.put(receptionPortURI, client);
				
				// Postcondition checks
				assert(registeredService(receptionPortURI));
				assert(registeredService(receptionPortURI, rc));
			}
			
		} finally {
			newClientLock.unlock();
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createRegisterMessage(getReflectionInboundPortURI(), receptionPortURI, rc);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
		
		if (rc == RegistrationClass.FREE) return publishingPortURI;
		else return privilegedPortURI;
	}

	/**
	 * Upgrade or downgrade the registration to the class {@code rc}.<br>
	 * Returns a new URI of an inbound port offering the component interface {@code PublishingCI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code registeredService(receptionPortURI, rc)}
	 * </pre>
	 *
	 * @param receptionPortURI	            URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				            the required registration class.
	 * @param fromLocalJVM					True if the call comes from a client in the same Broker JVM, and thus need to 
	 * 										be propagated with the gossip protocol.
	 * @return					            an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * @throws UnknownClientException       If the {@code receptionPortURI} is not registered at all.
	 * @throws AlreadyRegisteredException   If the client has already been registered with the exact same {@code rc}.
	 */
	public String modifyServiceClassService(String receptionPortURI, RegistrationClass rc, boolean fromLocalJVM) 
			throws UnknownClientException, AlreadyRegisteredException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()): 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(rc != null) : 
			new PreconditionException("registration class cannot be null");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client){
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered at all.");
			
			if (registeredService(receptionPortURI, rc)) // if the client is ALREADY registered with the SAME registration class
				throw new AlreadyRegisteredException("Client "+receptionPortURI+" already registered with the same registration class.");
		
			// Modify the client registration class
			client.setRegistrationClass(rc);
			
			String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
			this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+": Broker modifying service class for client : '"+receptionPortURI+"', now : "+rc);
	
			// Postcondition check
			assert(registeredService(receptionPortURI, rc));
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createUpdateClientClassMessage(getReflectionInboundPortURI(), receptionPortURI, rc);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
		
		if (rc == RegistrationClass.FREE) return publishingPortURI;
		else return privilegedPortURI;
	}

	/**
	 * Unregister from the publication/subscription system.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code !registeredService(receptionPortURI)}
	 * </pre>
	 *
	 * @param receptionPortURI	               	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param fromLocalJVM						True if the call comes from a client in the same Broker JVM, and thus need to 
	 * 											be propagated with the gossip protocol.
	 * @throws UnknownClientException          	If the {@code receptionPortURI} is not registered.
	 * @throws Exception                       	If there is a problem while unpublishing the component's outbound port associated 
	 * 											to the {@code receptionPortURI}.
	 * @throws Exception                       	If there is a problem while disconnecting outbound port and {@code receptionPortURI}.
	 */
	public void unregisterService(String receptionPortURI, boolean fromLocalJVM) 
			throws UnknownClientException, Exception {
		// Precondition check
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
			
			// Remove client infos
			clientsInfos.remove(receptionPortURI);

			// If the newly unregistered client is in the same JVM, disconnect and unpublish its associated port
			client.disconnectClientPort(this);
			client.unpublishClientPort();
			
			// Remove from subscribed channels the port previously registered
			for (Map.Entry<String, ChannelBrokerInfos> entry : channelsInfos.entrySet()) {
			    entry.getValue().removeSubscriber(receptionPortURI); // implicitly takes every channel lock
			}
			
			String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
			this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+": Broker unregistering '" + receptionPortURI +"'") ;
			
			// Postcondition check
			assert(!registeredService(receptionPortURI));
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createUnregisterMessage(getReflectionInboundPortURI(), receptionPortURI);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
	}

	/**
	 * Return true if {@code channel} exists, otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel		name of a potential channel.
	 * @return				true if {@code channel} exists, otherwise false.
	 */
	public boolean channelExistService(String channel) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		
		return channelsInfos.containsKey(channel);
	}
	
	/**
	 * Return true if the use of {@code channel} is authorized for {@code receptionPortURI}, otherwise false.
	 * Note that the creator is automatically authorized.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	           URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           name of a potential channel.
	 * @return					           true if the use of {@code channel} is authorised for {@code receptionPortURI}, otherwise false.
	 * @throws UnknownClientException      If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     If the {@code channel} doesn't exist.
	 */
	public boolean channelAuthorisedService(String receptionPortURI, String channel) 
			throws UnknownClientException, UnknownChannelException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered."); 
			
			// Gets the existing channel informations instance
			ChannelBrokerInfos chan = getExistingChannel(channel);
			
			return chan.isAuthorized(receptionPortURI);
		}
	}

	/**
	 * Return true if {@code receptionPortURI} has subscribed to {@code channel}, otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	           URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           name of an existing channel.
	 * @return					           true if {@code receptionPortURI} has subscribed to {@code channel}, otherwise false.
	 * @throws UnknownClientException      If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     If the {@code channel} doesn't exist.
	 */
	public boolean subscribedService(String receptionPortURI, String channel) 
			throws UnknownClientException, UnknownChannelException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
			
			// Gets the existing channel informations instance
			ChannelBrokerInfos chan = getExistingChannel(channel);
			
			return chan.isSubscribed(receptionPortURI);
		}
	}

	/**
	 * Subscribe to {@code channel} with filter {@code filter}.
	 * 
	 * <p>
	 * Subscription postconditions are delegated to the 
	 * {@link ChannelBrokerInfos#addOrReplaceSubscriber(String, MessageFilterI)} call.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code true} // no postcondition
	 * </pre>
	 *
	 * @param receptionPortURI       		URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			      		name of an existing channel.
	 * @param filter			           	filter that accepts the message the component wants to receive.
	 * @param fromLocalJVM					True if the call comes from a client in the same Broker JVM, and thus need to 
	 * 										be propagated with the gossip protocol.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException 	If {@code receptionPortURI} is not authorized to interact with the {@code channel}.
	 */
	public void subscribeService(String receptionPortURI, String channel, MessageFilterI filter, boolean fromLocalJVM) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(filter != null) : 
			new PreconditionException("filter cannot be null");

		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
			
			// Gets the existing channel informations instance
			ChannelBrokerInfos chan = getExistingChannel(channel);

			// Subscribe the client to the channel with the associated filter, if he is authorized to do so
			chan.addOrReplaceSubscriber(receptionPortURI, filter);
			
			String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
			this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+": Broker subscribing '" + receptionPortURI + "' to '"+ channel + "'");
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createSubscribeClientMessage(getReflectionInboundPortURI(), 
					receptionPortURI, channel, filter);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
	}

	/**
	 * Unsubscribe from {@code channel}.
	 * 
	 * <p>
	 * Subscription postcondition is delegated to the 
	 * {@link ChannelBrokerInfos#removeSubscriber(String)} call.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true} // no postcondition
	 * </pre>
	 *
	 * @param receptionPortURI	              	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			            	name of an existing channel.
	 * @param fromLocalJVM						True if the call comes from a client in the same Broker JVM, and thus need 
	 * 											to be propagated with the gossip protocol.
	 * @throws UnknownClientException			If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException      	If the {@code channel} doesn't exist.
	 * @throws NotSubscribedChannelException 	If {@code receptionPortURI} is not subscribed to the {@code channel}.
	 */
	public void unsubscribeService(String receptionPortURI, String channel, boolean fromLocalJVM) 
			throws UnknownClientException, UnknownChannelException, NotSubscribedChannelException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
			
			// Gets the existing channel informations instance
			ChannelBrokerInfos chan = getExistingChannel(channel);

			// Unsubscribe the client of the channel, if he is already subscribed to it
			chan.removeSubscriber(receptionPortURI);
				
			String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
			this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+": Broker unsubscribing '" + receptionPortURI + "' from '"+ channel + "'") ;
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createUnsubscribeClientMessage(getReflectionInboundPortURI(), 
					receptionPortURI, channel);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
	}

	/**
	 * Modify the filter placed on an existing {@code channel} to {@code filter}, 
	 * for a registered {@code receptionPortURI}, and subscribed to the {@code channel}.
	 * 
	 * <p>
	 * Filter modifications postcondition are delegated to the 
	 * {@link ChannelBrokerInfos#replaceSubscriber(String, MessageFilterI)} call.
	 * </p>
	 * 
	 * <p>
	 * There is no need to handle this with the gossip protocol : messages are only sent to local clients,
	 * so it is enough to modify the filter only on the local Broker without spreading the info.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	                URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel		                 	name of an existing channel.
	 * @param filter			                filter that accepts the message the component wants to receive.
	 * @throws UnknownClientException	     	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException          If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException 		If {@code receptionPortURI} is not authorized to interact with the {@code channel}.
	 * @throws NotSubscribedChannelException    If {@code receptionPortURI} is not subscribed to the {@code channel}.
	 */
	public void modifyFilterService(String receptionPortURI, String channel, MessageFilterI filter) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException, NotSubscribedChannelException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(filter != null) : 
			new PreconditionException("filter cannot be null");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
			
			// Gets the existing channel informations instance
			ChannelBrokerInfos chan = getExistingChannel(channel);
			
			// Overrides the existing filter with a new one, if subscribed and authorized to do so
			chan.replaceSubscriber(receptionPortURI, filter);

			this.logMessage("Thread "+Thread.currentThread().getId()+": '"+receptionPortURI+"' modifies its filter on channel '"+channel+"'");
		}
	}
	
	// PUBLICATIONS
	
	/**
	 * Publish all of the {@code messages} on an existing {@code channel} from a registered {@code receptionPortURI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.size() > 0}
	 * pre	{@code Stream.of(messages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	           	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           	name of the channel on which {@code message} must be published.
	 * @param messages			           	list of messages to be published on {@code channel}. It can contain an only message.
	 * @param fromLocalJVM					True if the call comes from a client in the same Broker JVM, and thus need to be 
	 * 										propagated with the gossip protocol.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException 	If {@code receptionPortURI} is not authorized to interact with the {@code channel}.
	 */
	public void publishService(String receptionPortURI, String channel, ArrayList<MessageI> messages, boolean fromLocalJVM) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(messages != null && messages.size() > 0) : 
			new PreconditionException("messages must at least contain 1 message");
		assert(Stream.of(messages).allMatch(m -> m != null)) : 
			new PreconditionException("a message cannot be null");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
			
			// Gets the existing channel informations instance
			ChannelBrokerInfos chan = getExistingChannel(channel);
				
			if (!chan.isAuthorized(receptionPortURI)) 
				throw new UnauthorisedClientException("Client "+receptionPortURI+" is not authorized.");
				
			String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
			this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+": Broker received "+messages.size()+" message(s) from '" + 
					receptionPortURI + "' on '" + channel + "'") ;
				
			// Delegates loading messages to a loaders thread pool, chosen depending on the registration class of the publisher
			RegistrationClass rc = client.getRegistrationClass();
			if (rc == RegistrationClass.FREE) {
				this.runTask(FREE_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, chan, channel, messages));
			} else if (rc == RegistrationClass.STANDARD) {
				this.runTask(STD_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, chan, channel, messages));
			} else {
				this.runTask(PREM_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, chan, channel, messages));
			}
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createPublishMessages(getReflectionInboundPortURI(), 
					receptionPortURI, channel, messages);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
	}
	
	// PRIVILEGED SERVICES
	
	/**
	 * Return true if the component has created {@code channel}.<br>
	 * The creator component is not necessarily still registered.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						channel to be tested.
	 * @return								true if the component has created {@code channel}.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 */
	public boolean hasCreatedChannelService(String receptionPortURI, String channel) 
			throws UnknownChannelException{
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		
		ChannelBrokerInfos chan = getExistingChannel(channel);
		return chan.isCreator(receptionPortURI);
	}

	/**
	 * Return true if the component registered under {@code receptionPortURI} has reached its channel quota.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI			URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @return							true if the component registered under {@code receptionPortURI} has reached its channel quota.
	 * @throws UnknownClientException   If the {@code receptionPortURI} is not registered.
	 */
	public boolean channelQuotaReachedService(String receptionPortURI) 
			throws UnknownClientException {
		// Precondition check
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		return client.canCreateChannel();
	}

	/**
	 * Create a new channel of the given name with a set of users authorized to use it.
	 * 
	 * <p>A channel creation lock is used to ensure not having 2 same channel name creations simultaneously.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code autorisedUsers == null || !autorisedUsers.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI						identifier of the requesting component <i>i.e.</i>, URI of its inbound port 
	 * 												offering the component interface {@code ReceivingCI}.
	 * @param channel								name of the channel to be created.
	 * @param autorizedUsers						regular expression that matches the URIs of components that must be authorized 
	 * 												to use the new channel.
	 * @param fromLocalJVM							True if the call comes from a client in the same Broker JVM, and thus need to be 
	 * 												propagated with the gossip protocol.
	 * @throws UnknownClientException   			If the {@code receptionPortURI} is not registered.
	 * @throws AlreadyExistingChannelException   	If the {@code channel} already exists.
	 * @throws ChannelQuotaExceededException   		If the {@code receptionPortURI} cannot currently manage more channels.
	 */
	public void createChannelService(String receptionPortURI, String channel, String autorizedUsers, boolean fromLocalJVM) 
			throws UnknownClientException,  AlreadyExistingChannelException, ChannelQuotaExceededException{
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(autorizedUsers != null && !autorizedUsers.isEmpty()) : 
			new PreconditionException("autorizedUsers cannot be null or empty");
		
		newChannelLock.lock();
		try {
			// Takes the client lock
			ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
			synchronized(client) {
				if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
					throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
				
				if (channelsInfos.containsKey(channel)) 
					throw new AlreadyExistingChannelException("Channel " + channel + " already exists.");
				if (!channelQuotaReachedService(receptionPortURI)) 
					throw new ChannelQuotaExceededException("No remaining channels to manage for "+receptionPortURI);
				
				String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
				this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+": Broker creating new channel '" + channel + "', owned by '" + receptionPortURI + "'") ;
				
				channelsInfos.put(channel, new ChannelBrokerInfos(channel, autorizedUsers, receptionPortURI));
				client.decreaseRemainingChannels();
			}
		} finally {
			newChannelLock.unlock();
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createCreateChannelMessage(getReflectionInboundPortURI(), 
					receptionPortURI, channel, autorizedUsers);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
	}

	/**
	 * Replace the users authorized to use {@code channel}, only available for the creator of the {@code channel}.
	 * 
	 * <p>
	 * After modifying the authorized users, users not matching anymore the new regex are unregistered from the {@code channel}.
	 * </p>
	 * 
	 * <p>
	 * Authorized users modification postcondition is delegated to the 
	 * {@link ChannelBrokerInfos#modifyAuthorizedUsers(String, String)} call.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code autorisedUsers != null && !autorisedUsers.isEmpty()}
	 * post	{@code true} // no postcondition
	 * </pre>
	 *
	 * @param receptionPortURI				identifier of the requesting component <i>i.e.</i>, URI of its inbound port 
	 * 										offering the component interface {@code ReceivingCI}.
	 * @param channel						name of the channel.
	 * @param autorizedUsers				regular expression that matches the URIs of components that must be authorized 
	 * 										to use the new channel.
	 * @param fromLocalJVM					True if the call comes from a client in the same Broker JVM, and thus need to be 
	 * 										propagated with the gossip protocol.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not the creator of the {@code channel}.
	 */
	public void modifyAuthorisedUsersService(String receptionPortURI, String channel, String autorizedUsers, boolean fromLocalJVM)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(autorizedUsers != null && !autorizedUsers.isEmpty()) : 
			new PreconditionException("autorizedUsers cannot be null or empty");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
			
			ChannelBrokerInfos chan = getExistingChannel(channel);
				
			// Updates the accepted regex if it is the creator doing it
			// Also delegates potential unregistrations to the method
			chan.modifyAuthorizedUsers(receptionPortURI, autorizedUsers);
			
			String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
			this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+": Broker modifying authorized users "
					+ "for channel '" + channel + "', now authorized : '" + autorizedUsers + "'");
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createUpdateChannelAuthURIsMessage(getReflectionInboundPortURI(), 
					receptionPortURI, channel, autorizedUsers);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
	}

	/**
	 * Destroy the given channel when no more messages are currently waiting to be transmitted.
	 * 
	 * <p>
	 * If other threads (doing our tasks) are currently loading/unloading messages of that {@code channel}, 
	 * they are stopped with a {@link UnknownChannelException}.<br>
	 * Then the current thread sends the remaining messages of the {@code channel}, by batches, not having any limited number of messages.<br>
	 * In other words : for each client, it sends him all messages targeting him in once.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						name of the channel to be destroyed.
	 * @param fromLocalJVM					True if the call comes from a client in the same Broker JVM, and thus need to be 
	 * 										propagated with the gossip protocol.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not allowed to interact with the {@code channel}.
	 */
	public void destroyChannelService(String receptionPortURI, String channel, boolean fromLocalJVM) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException{
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : 
			new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		ChannelBrokerInfos chan = null;
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");

			// Takes the channel lock
			chan = getExistingChannel(channel);
			synchronized(chan) {
				if (!channelExistService(channel)) // if the channel has been destroyed while waiting for the lock
					throw new UnknownChannelException("Channel "+channel+" doesn't exist anymore (destroyed).");
				
				if (!chan.isAuthorized(receptionPortURI)) 
					throw new UnauthorisedClientException("Client "+receptionPortURI+" not authorized");
				
				// Removal of the channel mapping
				channelsInfos.remove(channel);
				
				String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
				this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+"(destroyChannel): Broker destroying channel '" + channel + "'") ;
			}
		}
				
		// If the channel creator was a client, and he is still registered on the broker : his number of remaining channels is increased
		String channelCreator = chan.getCreator();
		if (channelCreator != null) {
			try {
				// Takes the channel creator lock
				ClientBrokerInfos clientChannelCreator = getRegisteredClient(channelCreator);
				synchronized(clientChannelCreator) {
					try {
						if (registeredService(channelCreator)) clientChannelCreator.increaseRemainingChannels();
						// Normally an exception would be throw if the creator is not FREE,
						// however here being the creator implies that he is not FREE, but privileged, so the exception in never actually thrown.
					} catch (ChannelQuotaExceededException e) {
						e.printStackTrace();
					}
				}
			} catch (UnknownClientException e) {
				// channel creator no more registered (call getRegisteredClient(channelCreator)) : nothing
			}
		}
				
		// Mapping : client target port URI -> messages to send to this target
		Map<String, List<MessageI>> targetsMessages = new HashMap<>();
				
		// Equivalent of the unloading task, done by the current thread
		MessageI msg = chan.unloadMessage();
		while (msg != null) {
					
			// Gets the channel clients targeted by this message AT THIS MOMENT + updates the map
			List<String> targetClientsPortURI = chan.targetSubscribers(msg);
			for (String target : targetClientsPortURI) {
				if (!targetsMessages.containsKey(target)) targetsMessages.put(target, new ArrayList<>());
				targetsMessages.get(target).add(msg);
			}
					
			msg = chan.unloadMessage();
		}
				
		// Equivalent of the sending task, for each targeted client, done by the current thread
		for(Map.Entry<String, List<MessageI>> entry : targetsMessages.entrySet()) {
			String targetPortURI = entry.getKey();
			List<MessageI> listMessagesToSend = entry.getValue();
					
			int l = listMessagesToSend.size();
			MessageI[] messagesToSend = new MessageI[l];
			for(int i=0; i<l; i++) messagesToSend[i] = listMessagesToSend.get(i);
					
			try {
				// Gets the registered target informations instance
				ClientBrokerInfos targetClient = getRegisteredClient(targetPortURI);
				
				// Assume that he is still registered while sending messages through its port, 
				// without taking its lock for better concurrent performances
				try {
					targetClient.sendMessages(channel, messagesToSend);
					logMessage("Thread "+Thread.currentThread().getId()+" (destroyChannel): Broker sending "+messagesToSend.length+" message(s) to '" + targetPortURI + "'") ;
				} catch (Exception e) {
					System.err.println("Sending error after destroying channel : "+e.getMessage());
				}
			} catch (UnknownClientException e) {
				// Thrown on getRegisteredClient(targetPortURI) if the target is not registered anymore : does nothing and continue
			}

		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createDestroyChannelMessage(getReflectionInboundPortURI(), 
					receptionPortURI, channel, false);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
	}

	/**
	 * Destroy the given channel immediately, even if the channel has still messages to be transmitted.
	 * 
	 * <p>
	 * If other threads (doing our tasks) are currently loading/unloading messages of that {@code channel}, 
	 * they are stopped with a {@link UnknownChannelException}.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						name of the channel to be destroyed.
	 * @param fromLocalJVM					True if the call comes from a client in the same Broker JVM, and thus need to be 
	 * 										propagated with the gossip protocol.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not allowed to interact with the {@code channel}.
	 */
	public void destroyChannelNowService(String receptionPortURI, String channel, boolean fromLocalJVM) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException{
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		// Takes the client lock
		ClientBrokerInfos client = getRegisteredClient(receptionPortURI);
		ChannelBrokerInfos chan = null;
		synchronized(client) {
			if (!registeredService(receptionPortURI)) // if the client has been unregistered while waiting for the lock
				throw new UnknownClientException("Client "+receptionPortURI+" not registered.");

			// Takes the channel lock
			chan = getExistingChannel(channel);
			synchronized(chan) {
				if (!channelExistService(channel)) // if the channel has been destroyed while waiting for the lock
					throw new UnknownChannelException("Channel "+channel+" doesn't exist anymore (destroyed).");
				
				if (!chan.isAuthorized(receptionPortURI)) 
					throw new UnauthorisedClientException("Client "+receptionPortURI+" not authorized");
				
				// Removal of the channel mapping
				channelsInfos.remove(channel);
				
				String gossipLog = !fromLocalJVM ? "[GOSSIP] " : "";
				this.logMessage(gossipLog+"Thread "+Thread.currentThread().getId()+"(destroyChannelNow): Broker destroying channel '" + channel + "'") ;
			}
		}
				
		// If the channel creator was a client, and he is still registered on the broker : his number of remaining channels is increased
		String channelCreator = chan.getCreator();
		if (channelCreator != null) {
			try {
				// Takes the channel creator lock
				ClientBrokerInfos clientChannelCreator = getRegisteredClient(channelCreator);
				synchronized(clientChannelCreator) {
					try {
						if (registeredService(channelCreator)) clientChannelCreator.increaseRemainingChannels();
						// Normally an exception would be throw if the creator is not FREE,
						// however here being the creator implies that he is not FREE, but privileged, so the exception in never actually thrown.
					} catch (ChannelQuotaExceededException e) {
						e.printStackTrace();
					}
				}
			} catch (UnknownClientException e) {
				// channel creator no more registered (call getRegisteredClient(channelCreator)) : nothing
			}
		}
		
		// Gossip sending
		if (fromLocalJVM) {
			GossipMessage message = GossipMessageFactory.createDestroyChannelMessage(getReflectionInboundPortURI(), 
					receptionPortURI, channel, true);
			this.runTask(Broker.GOSSIP_SENDING_POOL_URI, o -> {
				gossipPlugin.storeForSending(new GossipMessage[]{message});
			});
		}
	}
	
}
