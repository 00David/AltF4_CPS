package fr.sorbonne_u.broker.components;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import fr.sorbonne_u.broker.components.infos.ChannelBrokerInfos;
import fr.sorbonne_u.broker.components.infos.ClientBrokerInfos;
import fr.sorbonne_u.broker.components.tasks.LoaderTask;
import fr.sorbonne_u.broker.components.tasks.SenderTask;
import fr.sorbonne_u.broker.components.tasks.UnloaderTask;
import fr.sorbonne_u.broker.connectors.BrokerReceivingServiceConnector;
import fr.sorbonne_u.broker.ports.BrokerReceivingOutboundPort;
import fr.sorbonne_u.broker.ports.BrokerPrivilegedInboundPort;
import fr.sorbonne_u.broker.ports.BrokerPublishingInboundPort;
import fr.sorbonne_u.broker.ports.BrokerRegistrationInboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
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

/**
 * Component implementing the publish/subscribe system.
 * 
 * <p>About the publication service : </p>
 * <ul>
 *   <li>It is firstly handled by a thread from the classic thread pool, mostly verifying service preconditions.</li>
 *   <li>Then it is handled by a thread from one of loading thread pools, loading messages into a channel. 
 *   		Corresponding task : {@link LoaderTask}</li>
 *   <li>Then it is handled by a thread from the unloading thread pool, unloading messages from a channel.
 *    		Corresponding task : {@link UnloaderTask}</li>
 *   <li>Finally, it is handled by a thread from the sending thread pool, sending messages previously unloaded.
 *   		Corresponding task : {@link SenderTask}</li>
 * </ul>
 * 
 * <p><strong>Main features : </strong></p>
 * <ul>
 *   <li>Manages client component registrations (via the URI of their inbound port implementing {@link ReceivingCI}).</li>
 *   <li>Manages transmission channels between these registered client components.</li>
 *   <li>Handles message transmissions by applying filters provided by components subscribed to the relevant channel.</li>
 *   <li>Handles privileges for non-FREE clients, with better performances on publication and advanced channel management.
 *   <p> More precisely about performances, there are differences on 2 layers :
 *   	<ul>
 *			<li>Depending on the registration class of the publishing client, a different loading thread pool is used.
 *				Higher-privileged registration classes have larger loading thread pools (scaled on the size of the FREE loading pool).</li>
 *			<li>Depending on the registration class of the creator of a channel, messages sent from it can be grouped.
 *				Higher-privileged channel creator registration classes allows biggest groups of messages sent together 
 *				(for a FREE channel creator, it is only 1 message per sending).</li>
 *   	</ul>
 *   </li>
 * </ul>
 * 
 * <p>
 * Each provided service is multi-thread safe : it either explicitly takes the required client / channel locks, introducing critical sections,
 * or when a single operation needs to be performed on an internal map (clientsInfos or channelsInfos), it can do the operation
 * without taking explicitly the lock, but directly doing the operation on the map, implemented by a {@link ConcurrentHashMap}
 * so multi-thread safe for read/write operations on it.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
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
 * invariant	{@code PREM_LOADERS_POOL_URI != null && !PREM_LOADERS_POOL_URI.isEmpty() && nbPremLoadersThreads > 0}
 * invariant	{@code UNLOADERS_POOL_URI != null && !UNLOADERS_POOL_URI.isEmpty() && nbUnloadersThreads > 0}
 * invariant	{@code SENDERS_POOL_URI != null && !SENDERS_POOL_URI.isEmpty() && nbSendersThreads > 0}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
@OfferedInterfaces(offered = {RegistrationCI.class, PublishingCI.class, PrivilegedClientCI.class})
@RequiredInterfaces(required = {ReceivingCI.class})
public class Broker extends AbstractComponent{
	
	//--------------------------------------------------------------------------
	// Internal datas
	//--------------------------------------------------------------------------
	
	// REGISTRATION
	
	/** Broker's inbound registration port, got by static method 'registrationPortURI' */
	private static final String registrationPortURI = "registration-inbound--port-broker";
	
	/** Broker's inbound port, implementing {@link RegistrationCI} */
	private final BrokerRegistrationInboundPort registrationPort;
	
	/**
	 * Public getter for allowing clients to register on the broker.
	 * @return The URI of Broker's {@link BrokerRegistrationInboundPort}.
	 */
	public static String registrationPortURI() { return registrationPortURI;}
	
	// PUBLICATION
	
	/** Broker's inbound publication port, got by registering an inbound client port */
	private static final String publishingPortURI = "publishing-inbound-port-broker";
	
	/** Broker's inbound port, implementing {@link PublishingCI} */
	private final BrokerPublishingInboundPort publishingPort;
	
	// PRIVILEGED MANAGEMENT
	
	/** Broker's inbound privileged port, got by registering an inbound privileged client port */
	private static final String privilegedPortURI = "privileged-inbound-port-broker";
	
	/** Broker's inbound port, implementing {@link PrivilegedClientCI} */
	private final BrokerPrivilegedInboundPort privilegedPort;

	// DATA MAPPINGS
	
	/** 
	 * Mapping : client inbound port (URI) -> its informations.<br>
	 * Multi-thread safe : implemented by a {@link ConcurrentHashMap}. An operation on it does not lock the whole map.
	 */
	private Map<String, ClientBrokerInfos> clientsInfos = new ConcurrentHashMap<>(); 
	
	/** 
	 * Mapping : channel name -> its informations.<br>
	 * Multi-thread safe : implemented by a {@link ConcurrentHashMap}. An operation on it does not lock the whole map.
	 */
	private Map<String, ChannelBrokerInfos> channelsInfos = new ConcurrentHashMap<>();
	
	// CONCURRENT PUBLISHING PERFORMANCES
	
	// For loading messages into channels, once received on the Broker + creating unloaders. 
	// A different pool according to the registration class of the publisher.
	
	/** URI of the pool of threads used to load messages into a channel, coming from a FREE client. */
	public static final String	FREE_LOADERS_POOL_URI = "free-loaders-pool" ;
	
	/** Number of threads in the {@code FREE_LOADERS_POOL_URI} pool. */
	private final int nbFreeLoadersThreads;
	
	/** URI of the pool of threads used to load messages into a channel, coming from a STANDARD client. */
	public static final String	STD_LOADERS_POOL_URI = "std-loaders-pool" ;
	
	/** Number of threads in the {@code STD_LOADERS_POOL_URI} pool. 2 x {@code nbFreeLoadersThreads} */
	private final int nbStdLoadersThreads;
	
	/** URI of the pool of threads used to load messages into a channel, coming from a PREMIUM client. */
	public static final String	PREM_LOADERS_POOL_URI = "prem-loaders-pool" ;
	
	/** Number of threads in the {@code PREM_LOADERS_POOL_URI} pool. 4 x {@code nbFreeLoadersThreads} */
	private final int nbPremLoadersThreads;
	
	// For unloading messages from channels + creating senders.
	
	/** URI of the pool of threads used to unload messages from channels. */
	public static final String	UNLOADERS_POOL_URI = "unloaders-pool" ;
	
	/** Number of threads in the {@code UNLOADERS_POOL_URI} pool. */
	private final int nbUnloadersThreads;
	
	// For sending unloaded messages to a targeted client.
	
	/** URI of the pool of threads used to send messages. */
	public static final String	SENDERS_POOL_URI = "senders-pool" ;
	
	/** Number of threads in the {@code SENDERS_POOL_URI} pool. */
	private final int nbSendersThreads;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	/**
	 * Creates a Broker with 'N' channels.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code N >= 2}
	 * pre	{@code nbThreads > 0}
	 * pre	{@code nbSchedulableThreads >= 0}
	 * pre	{@code nbFreeLoadersThreads > 0 && nbUnloadersThreads > 0 && nbSendersThreads > 0}
	 * post class invariants satisfied
	 * post outbound ports configured and initialized
	 * post {@code ClientBrokerInfos.nbMaxChannelsStd >= 0 && ClientBrokerInfos.nbMaxChannelsPrem >= 0}
	 * </pre>
	 * 
	 * @param N						The number of default channels in the Broker (from 'channel0' to 'channel[N-1]').
	 * @param nbMaxChannelsStd		The maximum number of channels that can be managed by a standard privileged client.
	 * @param nbMaxChannelsPrem		The maximum number of channels that can be managed by a premium privileged client.
	 * @param nbThreads				Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads	Number of threads to be created in the component schedulable pool.
	 * @param nbFreeLoadersThreads	Number of threads to be created in the free loaders pool. It is x2 for standard clients, and x4 for premium clients.
	 * @param nbUnloadersThreads	Number of threads to be created in the unloaders pool.
	 * @param nbSendersThreads		Number of threads to be created in the senders pool.
	 * @throws Exception			If there is a problem while publishing a port.
	 */
	protected Broker(int N, int nbMaxChannelsStd, int nbMaxChannelsPrem, int nbThreads, int nbSchedulableThreads,
			int nbFreeLoadersThreads, int nbUnloadersThreads, int nbSendersThreads) throws Exception {
		super(nbThreads, nbSchedulableThreads);
		// Precondition checks
		assert(N>=2) : new PreconditionException("Broker's number of default channels must be at least 2 ('channel0' for wind datas, 'channel1' for meteo alerts)");
		assert(nbThreads > 0) : new PreconditionException("must have at leat 1 normal thread");
		assert(nbSchedulableThreads >= 0) : new PreconditionException("cannot have a negative number of schedulable threads");
		assert(nbFreeLoadersThreads > 0 && nbUnloadersThreads > 0 && nbSendersThreads > 0) : new PreconditionException("must have at least 1 thread for each publication step");
		
		ClientBrokerInfos.nbMaxChannelsStd = nbMaxChannelsStd;
		ClientBrokerInfos.nbMaxChannelsPrem = nbMaxChannelsPrem;
		
		// Creation of N channels
		// The channel0 is for wind datas, and the channel1 for meteo alerts
		for (int i = 0; i < N; i++) {
			String chan = "channel"+i;
			channelsInfos.put(chan, new ChannelBrokerInfos(chan, ".*", null, false));
		}
		
		// Creation of thread pools
		this.nbFreeLoadersThreads = nbFreeLoadersThreads;
		this.nbStdLoadersThreads = 2*nbFreeLoadersThreads;
		this.nbPremLoadersThreads = 4*nbFreeLoadersThreads;
		this.nbUnloadersThreads = nbUnloadersThreads;
		this.nbSendersThreads = nbSendersThreads;
		
		this.createNewExecutorService(FREE_LOADERS_POOL_URI, this.nbFreeLoadersThreads, false);
		this.createNewExecutorService(STD_LOADERS_POOL_URI, this.nbStdLoadersThreads, false);
		this.createNewExecutorService(PREM_LOADERS_POOL_URI, this.nbPremLoadersThreads, false);
		this.createNewExecutorService(UNLOADERS_POOL_URI, this.nbUnloadersThreads, false);
		this.createNewExecutorService(SENDERS_POOL_URI, this.nbSendersThreads, false);

		// Ports creation and publication
		this.registrationPort = new BrokerRegistrationInboundPort(registrationPortURI, this);
		this.registrationPort.publishPort();
		
		this.publishingPort = new BrokerPublishingInboundPort(publishingPortURI, this);
		this.publishingPort.publishPort();
		
		this.privilegedPort = new BrokerPrivilegedInboundPort(privilegedPortURI, this);
		this.privilegedPort.publishPort();
		
		// Invariant checks
		assert	this.isOfferedInterface(RegistrationCI.class) :
			new InvariantException("The Broker component should offer the interface RegistrationCI!");
		assert	this.isOfferedInterface(PublishingCI.class) :
			new InvariantException("The Broker component should offer the interface PublishingCI!");
		assert	this.isOfferedInterface(PrivilegedClientCI.class) :
			new InvariantException("The Broker component should offer the interface PrivilegedClientCI!");
		assert	this.isRequiredInterface(ReceivingCI.class) :
			new InvariantException("The Broker component should require the interface ReceivingCI!");
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
		// Postcondition checks
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
		
		this.getTracer().setTitle("Broker");
		this.getTracer().setRelativePosition(0, 4);
	}
	
	//--------------------------------------------------------------------------
	// Component life-cycle
	//--------------------------------------------------------------------------

	@Override
	public synchronized void start() throws ComponentStartException {
		this.logMessage("starting Broker component.");
		super.start();
	}

	@Override
	public synchronized void finalise() throws Exception {
		this.logMessage("stopping Broker component.");
		
		// Disconnect all outbound ports linked to registered clients
		for (Map.Entry<String, ClientBrokerInfos> entry : clientsInfos.entrySet()) {
			BrokerReceivingOutboundPort outboundPort = entry.getValue().getOutboundPort();
			this.doPortDisconnection(outboundPort.getPortURI());
		}
		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		
		try {
			// Unpublish all outbound ports linked to registered clients
			for (Map.Entry<String, ClientBrokerInfos> entry : clientsInfos.entrySet()) {
				BrokerReceivingOutboundPort outboundPort = entry.getValue().getOutboundPort();
				outboundPort.unpublishPort();
			}
		
			// Unpublish all Broker inbound ports
			this.registrationPort.unpublishPort();
			this.publishingPort.unpublishPort();
			this.privilegedPort.unpublishPort();
		} catch (Throwable e) {
			throw new ComponentShutdownException(e);
		}
		
		super.shutdown();
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
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		
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
	 * @param receptionPortURI	           URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				           the registration class.
	 * @return					           true if {@code receptionPortURI} has already been registered, with service class {@code rc} if given, otherwise false.
	 * @throws UnknownClientException      If the {@code receptionPortURI} is not registered.
	 */
	public boolean registeredService(String receptionPortURI, RegistrationClass rc) 
			throws UnknownClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(rc != null) : new PreconditionException("registration class cannot be null");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		
		return clientsInfos.get(receptionPortURI).getRegistrationClass().equals(rc);
	}

	/**
	 * Register on the publication/subscription system passing the URI of the inbound port offering the component interface {@code ReceivingCI}.<br>
	 * Returns an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code registeredService(receptionPortURI)}
	 * post	{@code registeredService(receptionPortURI, rc)}
	 * </pre>
	 *
	 * @param receptionPortURI	          URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				          the required registration class.
	 * @return					          an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * @throws AlreadyRegisteredException If the client has already been registered.
	 * @throws UnknownClientException      If the {@code receptionPortURI} is not registered.
	 */
	public String registerService(String receptionPortURI, RegistrationClass rc) 
			throws AlreadyRegisteredException, UnknownClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(rc != null) : new PreconditionException("registration class cannot be null");
		
		if (registeredService(receptionPortURI)) throw new AlreadyRegisteredException("Client "+receptionPortURI+" already registered.");
		
		this.logMessage("Broker registering '" + receptionPortURI + "'") ;
		
		BrokerReceivingOutboundPort outboundPort = null;
		// Create the Broker outbound port for sending messages to the receptionPortURI
		// New outbound port published + connected to the client receptionPortURI
		try {
			outboundPort = new BrokerReceivingOutboundPort("BrokerOutboundPort-"+receptionPortURI, this);
			outboundPort.localPublishPort() ;
			this.doPortConnection(outboundPort.getPortURI(), receptionPortURI, BrokerReceivingServiceConnector.class.getCanonicalName());
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		
		// Add the receptionPortURI client infos to the mapping of the client infos
		// Takes the client lock
		ClientBrokerInfos client = new ClientBrokerInfos(receptionPortURI, rc, outboundPort);
		synchronized(client) {
			clientsInfos.put(receptionPortURI, client);
			
			// Postcondition checks
			assert(registeredService(receptionPortURI));
			assert(registeredService(receptionPortURI, rc));

			if (rc == RegistrationClass.FREE) return publishingPortURI;
			else return privilegedPortURI;
		}
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
	 * @return					            an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * @throws UnknownClientException       If the {@code receptionPortURI} is not registered at all.
	 * @throws AlreadyRegisteredException   If the client has already been registered with the exact same {@code rc}.
	 */
	public String modifyServiceClassService(String receptionPortURI, RegistrationClass rc) 
			throws UnknownClientException, AlreadyRegisteredException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()): new PreconditionException("receptionPortURI cannot be null or empty");
		assert(rc != null) : new PreconditionException("registration class cannot be null");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered at all.");
		// Takes the client lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client){
			if (registeredService(receptionPortURI, rc)) throw new AlreadyRegisteredException("Client "+receptionPortURI+" already registered with the same registration class.");
		
			this.logMessage("Broker modifying service class for client : '"+receptionPortURI+"', now : "+rc);
		
			// Modify the client registration class
			client.setRegistrationClass(rc);
		
			// Gestion when a client modifies its registration class while he is already subscribed to channels
			// We iterate over all pairs : channel name -> its infos
			for (Map.Entry<String, ChannelBrokerInfos> entry : channelsInfos.entrySet()) {
				String channel = entry.getKey();
				// Takes the channel lock
				ChannelBrokerInfos chanInfos = entry.getValue();
				synchronized (chanInfos) {
				
					// If our client is subscribed, we look if the channel accepts him with its new registration class
					if (chanInfos.isSubscribed(receptionPortURI)) {
						try {
							// If it is not the case, we remove him as a subscriber of the channel
							if (!channelAuthorisedService(receptionPortURI, channel)) {
								chanInfos.removeSubscriber(receptionPortURI);
								this.logMessage("client '"+receptionPortURI+"' unsubscribed from '"+chanInfos+"' because of new service class");
							}
						} catch (UnknownChannelException e) {
							System.err.println("Impossible, channel necessarily exists here");
						}
					}
				}
			}
	
			// Postcondition check
			assert(registeredService(receptionPortURI, rc));
			
			if (rc == RegistrationClass.FREE) return publishingPortURI;
			else return privilegedPortURI;
		}
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
	 * @throws UnknownClientException          	If the {@code receptionPortURI} is not registered.
	 * @throws Exception                       	If there is a problem while unpublishing the component's outbound port associated to the {@code receptionPortURI}.
	 * @throws Exception                       	If there is a problem while disconnecting outbound port and {@code receptionPortURI}.
	 * @return 									Void
	 */
	public Void unregisterService(String receptionPortURI) 
			throws UnknownClientException, Exception {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
		
			this.logMessage("Broker unregistering '" + receptionPortURI +"'") ;

			BrokerReceivingOutboundPort outboundPort = client.getOutboundPort();
			
			// Remove client infos
			clientsInfos.remove(receptionPortURI);

			// Disconnect the outbound port from the given inbound port (receptionPortURI)
			this.doPortDisconnection(outboundPort.getPortURI());
			
			// Unpublish the outbound port
			outboundPort.unpublishPort();
			
			// Remove from subscribed channels the port previously registered
			for (Map.Entry<String, ChannelBrokerInfos> entry : channelsInfos.entrySet()) {
			    entry.getValue().removeSubscriber(receptionPortURI); // implicitly takes every channel lock
			}
			
			// Postcondition check
			assert(!registeredService(receptionPortURI));
		}
		
		return null;
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
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		return channelsInfos.containsKey(channel);
	}
	
	/**
	 * Return true if the use of {@code channel} is authorised for {@code receptionPortURI}, otherwise false.
	 * 
	 * <p>
	 * The method only verifies the compatibily between the {@code receptionPortURI}'s {@link RegistrationClass}
	 * and the {@code channel}'s requirement to be accessed (or not) by only privileged clients.<br>
	 * 
	 * To include also regex verification on {@code channel}'s authorized users, please see the {@code isAuthorisedUserService} method.
	 * </p>
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
	 * @see Broker#isAuthorisedUserService(String, String)
	 */
	public boolean channelAuthorisedService(String receptionPortURI, String channel) 
			throws UnknownClientException, UnknownChannelException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock + the channel lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				if (client.getRegistrationClass() == RegistrationClass.FREE && channelsInfos.get(channel).onlyPrivileged()){
					return false;
				} else {
					return true;
				}
				
			}
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
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock + the channel lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				return chan.isSubscribed(receptionPortURI);
				
			}
		}
	}

	/**
	 * Subscribe to {@code channel} with filter {@code filter}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code subscribedService(receptionPortURI, channel)}
	 * </pre>
	 *
	 * @param receptionPortURI       		URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			      		name of an existing channel.
	 * @param filter			           	filter that accepts the message the component wants to receive.
	 * @return 									Void
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException 	If {@code receptionPortURI} is not authorized to interact with the {@code channel}.
	 */
	public Void subscribeService(String receptionPortURI, String channel, MessageFilterI filter) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(filter != null) : new PreconditionException("filter cannot be null");

		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock + the channel lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				if (!channelAuthorisedService(receptionPortURI, channel)) throw new UnauthorisedClientException("Client "+receptionPortURI+" is not authorized.");
				
				this.logMessage("Broker subscribing '" + receptionPortURI + "' to '"+ channel + "'") ;
			
				// Subscibe the client to the channel, with the associated filter
				chan.addOrReplaceSubscriber(receptionPortURI, filter);
				
				// Postcondition check
				assert(subscribedService(receptionPortURI, channel));
				
			}
		}
		
		return null;
	}

	/**
	 * Unsubscribe from {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code !subscribedService(receptionPortURI, channel)}
	 * </pre>
	 *
	 * @param receptionPortURI	              	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			            	name of an existing channel.
	 * @return 									Void
	 * @throws UnknownClientException			If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException      	If the {@code channel} doesn't exist.
	 * @throws NotSubscribedChannelException 	If {@code receptionPortURI} is not subscribed to the {@code channel}.
	 */
	public Void unsubscribeService(String receptionPortURI, String channel) 
			throws UnknownClientException, UnknownChannelException, NotSubscribedChannelException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock + the channel lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				if (!subscribedService(receptionPortURI, channel)) throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
				
				this.logMessage("Broker unsubscribing '" + receptionPortURI + "' from '"+ channel + "'") ;
				
				// Unsubscribe the client of the channel
				chan.removeSubscriber(receptionPortURI);
				
				// Postcondition check
				assert(!subscribedService(receptionPortURI, channel));
				
			}
		}
		
		return null;
	}

	/**
	 * Modify the filter placed on an existing {@code channel} to {@code filter}, 
	 * for a registered {@code receptionPortURI}, and subscribed to the {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	                 URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel		                 	 name of an existing channel.
	 * @param filter			                 filter that accepts the message the component wants to receive.
	 * @return					                 true if the modification has been performed, otherwise false.
	 * @throws UnknownClientException	     	 If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException           If the {@code channel} doesn't exist.
	 * @throws NotSubscribedChannelException     If {@code receptionPortURI} is not subscribed to the {@code channel}.
	 */
	public boolean modifyFilterService(String receptionPortURI, String channel, MessageFilterI filter) 
			throws UnknownClientException, UnknownChannelException, NotSubscribedChannelException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(filter != null) : new PreconditionException("filter cannot be null");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				if (!subscribedService(receptionPortURI, channel)) throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
				
				this.logMessage("'"+receptionPortURI+"' modifies its filter on channel '"+channel+"'");
				
				// Overrides the existing filter with a new one
				chan.addOrReplaceSubscriber(receptionPortURI, filter);
				
			}
		}
		
		return true;
	}
	
	// PUBLICATIONS
	
	/**
	 * Publish all of the {@code messages} on an existing {@code channel} from a registered {@code receptionPortURI}.<br>
	 * For now, also send those {@code messages} to clients accepting them after applying their filters.
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
	 * @return 								Void
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException 	If {@code receptionPortURI} is not authorized to interact with the {@code channel}.
	 */
	public Void publishService(String receptionPortURI, String channel, ArrayList<MessageI> messages) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(messages != null && messages.size() > 0) : new PreconditionException("messages must at least contain 1 message");
		assert(Stream.of(messages).allMatch(m -> m != null)) : new PreconditionException("a message cannot be null");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock + the channel lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				if (!channelAuthorisedService(receptionPortURI, channel)) throw new UnauthorisedClientException("Client "+receptionPortURI+" is not authorized.");
				
				this.logMessage("Broker received message(s) from '" + receptionPortURI + "' on '" + channel + "'") ;
				
				// Delegates loading messages to a loaders thread pool, chosen depending on the registration class of the publisher
				RegistrationClass rc = client.getRegistrationClass();
				if (rc == RegistrationClass.FREE) {
					this.runTask(FREE_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, channelsInfos, channel, messages));
				} else if (rc == RegistrationClass.STANDARD) {
					this.runTask(STD_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, channelsInfos, channel, messages));
				} else {
					this.runTask(PREM_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, channelsInfos, channel, messages));
				}
				
			}
		}
		return null;
	}
	
	/**
	 * Asynchronously publish all of the {@code messages} on {@code channel} and notify the client component if an 
	 * abnormal termination condition occurred.
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
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						name of the channel on which {@code message} must be published.
	 * @param messages						list of messages to be published on {@code channel}.
	 * @param notificationInbounhdPortURI	URI of an inbound port offering {@code AbnormalTerminationNotificationCI}.
	 * @return 								Void
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException 	If {@code receptionPortURI} is not authorized to interact with the {@code channel}.
	 */
	public Void asyncPublishAndNotifyService(String receptionPortURI, String channel, ArrayList<MessageI> messages, String notificationInbounhdPortURI) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(messages != null && messages.size() > 0) : new PreconditionException("messages must at least contain 1 message");
		assert(Stream.of(messages).allMatch(m -> m != null)) : new PreconditionException("a message cannot be null");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock + the channel lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				if (!channelAuthorisedService(receptionPortURI, channel)) throw new UnauthorisedClientException("Client "+receptionPortURI+" is not authorized.");
				// TODO : send exceptions via notificationInbounhdPortURI
				
				this.logMessage("Broker received message(s) from '" + receptionPortURI + "' on '" + channel + "'") ;
				
				// Delegates loading messages to a loaders thread pool, chosen depending on the registration class of the publisher
				RegistrationClass rc = clientsInfos.get(receptionPortURI).getRegistrationClass();
				if (rc == RegistrationClass.FREE) {
					this.runTask(FREE_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, channelsInfos, channel, messages));
				} else if (rc == RegistrationClass.STANDARD) {
					this.runTask(STD_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, channelsInfos, channel, messages));
				} else {
					this.runTask(PREM_LOADERS_POOL_URI, new LoaderTask(this, clientsInfos, channelsInfos, channel, messages));
				}
				
			}
		}
		
		return null;
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
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		
		return channelsInfos.get(channel).getCreator().equals(receptionPortURI);
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
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		
		return clientsInfos.get(receptionPortURI).canCreateChannel();
	}

	/**
	 * Create a new channel of the given name with a set of users authorized to use it.
	 * 
	 * <p>
	 * The newly created channel is only accessible to clients with a STANDARD or PREMIUM {@link RegistrationClass}.<br>
	 * (last parameter given to {@link ChannelBrokerInfos}'s constructor is {@code true}).
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code autorisedUsers == null || !autorisedUsers.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI						identifier of the requesting component <i>i.e.</i>, URI of its inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel								name of the channel to be created.
	 * @param autorizedUsers						regular expression that matches the URIs of components that must be authorized to use the new channel.
	 * @return 										Void
	 * @throws UnknownClientException   			If the {@code receptionPortURI} is not registered.
	 * @throws AlreadyExistingChannelException   	If the {@code channel} already exists.
	 * @throws ChannelQuotaExceededException   		If the {@code receptionPortURI} cannot currently manage more channels.
	 */
	public Void createChannelService(String receptionPortURI, String channel, String autorizedUsers) 
			throws UnknownClientException,  AlreadyExistingChannelException, ChannelQuotaExceededException{
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(autorizedUsers != null && !autorizedUsers.isEmpty()) : new PreconditionException("autorizedUsers cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			
			if (channelsInfos.containsKey(channel)) throw new AlreadyExistingChannelException("Channel " + channel + " already exists.");
			if (!channelQuotaReachedService(receptionPortURI)) throw new ChannelQuotaExceededException("No remaining channels to manage for "+receptionPortURI);
			
			this.logMessage("Broker creating new channel '" + channel + "', owned by '" + receptionPortURI + "'") ;
			
			channelsInfos.put(channel, new ChannelBrokerInfos(channel, autorizedUsers, receptionPortURI, true));
			client.decreaseRemainingChannels();
			return null;
			
		}
	}

	/**
	 * Return true if {@code uri} is authorized to use {@code channel}. The creator is automatically authorized.
	 * 
	 * <p>
	 * Calls the method {@code channelAuthorisedService} before veryfing the regex for the {@code channel}'s authorized users,
	 * so that {@code uri}'s {@link RegistrationClass} is compatible with the {@code channel}'s requirement to be accessed 
	 * (or not) by only privileged clients
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code uri != null && !uri.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel						name of an existing channel.
	 * @param uri							URI of component to be tested.
	 * @return								true if {@code uri} is authorized to use {@code channel}.
	 * @throws UnknownClientException   	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @see Broker#channelAuthorisedService(String, String)
	 */
	public boolean isAuthorisedUserService(String channel, String uri) 
			throws UnknownClientException, UnknownChannelException {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("uri cannot be null or empty");
		
		if (!registeredService(uri)) throw new UnknownClientException("Client "+uri+" not registered.");
		// Takes the client lock
		ClientBrokerInfos client = clientsInfos.get(uri);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				// If the client is the creator of the channel, he is automatically authorized
				if (chan.getCreator().equals(uri)) return true;

				// This call checks that the RegistrationClass of the URI is compatible with the channel
				if (!channelAuthorisedService(uri, channel)) return false;
				
				// Gets the regex accepting clients URIs
				String authorised = chan.getAuthorizedUsers();
						
				return uri.matches(authorised);
				
			}
		}
	}

	/**
	 * Replace the users authorized to use {@code channel}, only available for the creator of the {@code channel}.
	 * 
	 * <p>
	 * After modyfing the authorized users, unregister users not matching anymore the new regex.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code autorisedUsers != null && !autorisedUsers.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				identifier of the requesting component <i>i.e.</i>, URI of its inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						name of the channel.
	 * @param autorizedUsers				regular expression that matches the URIs of components that must be authorized to use the new channel.
	 * @return 								Void
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not the creator of the {@code channel}.
	 */
	public Void modifyAuthorisedUsersService(String receptionPortURI, String channel, String autorizedUsers)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(autorizedUsers != null && !autorizedUsers.isEmpty()) : new PreconditionException("autorizedUsers cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock + the channel lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			ChannelBrokerInfos chan = channelsInfos.get(channel);
			synchronized(chan) {
				
				// If the client is not the creator of the channel, then he cannot modify authorized users.
				if (!chan.getCreator().equals(receptionPortURI)) throw new UnauthorisedClientException("Client "+receptionPortURI+" is not the creator of the channel : '"+channel+"'");
				
				this.logMessage("Broker modifying authorized users for channel '" + channel + "', now authorized : '" + autorizedUsers + "'") ;
				
				// Updates the accepted regex and delegates potential unregistrations to the method
				chan.modifyAuthorizedUsers(autorizedUsers);
				
			}
		}
		return null;
	}

	/**
	 * Destroy the given channel when no more messages are currently waiting to be transmitted.
	 * 
	 * <p>
	 * If other threads are currently publishing messages of that {@code channel}, 
	 * they are stopped with a {@link UnknownChannelException}.<br>
	 * Then the current thread sends the remaining messages of the {@code channel}.
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
	 * @return 								Void
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not allowed to interact with the {@code channel}.
	 */
	public Void destroyChannelService(String receptionPortURI, String channel) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException{
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			if (!isAuthorisedUserService(channel, receptionPortURI)) throw new UnauthorisedClientException("Client "+receptionPortURI+" not authorized");
			
			// Removal of the channel mapping
			ChannelBrokerInfos chanInfos = channelsInfos.remove(channel);
			if (chanInfos == null) throw new UnknownChannelException("Channel "+channel+" doesn't exist (removed in parallel).");
			
			// If the channel creator was a client, and he is still registered on the broker : his number of remaining channels is increased
			String channelCreator = chanInfos.getCreator();
			if (channelCreator != null) {
				// Takes the channel creator lock
				ClientBrokerInfos clientChannelCreator = clientsInfos.get(channelCreator);
				synchronized(clientChannelCreator) {
					try {
						if (registeredService(channelCreator)) clientChannelCreator.increaseRemainingChannels();
						// Normally an exception would be throw if the creator is not FREE,
						// however here being the creator implies that he is not FREE, but privileged, so the exception in never actually thrown.
					} catch (ChannelQuotaExceededException e) {
						e.printStackTrace();
					}
				}
			}
			
			// Sends remaining messages (one after one)
			MessageI msg = chanInfos.unloadMessage();
			while (msg != null) {
				
				final MessageI currentMsg = msg;
				
				// Gets the channel clients targeted by this message AT THIS MOMENT
				List<String> targetClientsPortURI = chanInfos.targetSubscribers(currentMsg);
				
				// Sends them the message
				for (String targetPortURI : targetClientsPortURI) {
					
					// Checks that the current targeted client is still registered and subscribed to the channel
					if (registeredService(targetPortURI) && subscribedService(targetPortURI, channel)) {
						
						this.logMessage("Broker sending message to '" + targetPortURI + "'") ;
						BrokerReceivingOutboundPort outboundPort = client.getOutboundPort();
						try {
							outboundPort.receive(channel, currentMsg);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}

		
		// Dans le cahier des charges il est mentionné que les ports entrants offrant ReceivingCI doivent être déconnectés après 
		// destruction d'un channel, mais cela semble être trop pénalisant/pas naturel pour un tel système, du coup on fait le choix
		// de garder les ports connectés, mais via la destruction du mapping du channel d'implicitement unsubscribe les clients du channel, 
		// sans donc complètement tous les déconnecter du système
		
		return null;
	}

	/**
	 * Destroy the given channel immediately, even if the channel has still messages to be transmitted.
	 * 
	 * <p>
	 * If other threads are currently publishing messages of that {@code channel}, 
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
	 * @return 								Void
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not allowed to interact with the {@code channel}.
	 */
	public Void destroyChannelNowService(String receptionPortURI, String channel) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException{
		// Precondition checks
		assert(receptionPortURI != null && !receptionPortURI.isEmpty()) : new PreconditionException("receptionPortURI cannot be null or empty");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		// Takes the client lock
		ClientBrokerInfos client = clientsInfos.get(receptionPortURI);
		synchronized(client) {
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
			if (!isAuthorisedUserService(channel, receptionPortURI)) throw new UnauthorisedClientException("Client "+receptionPortURI+" not authorized");
			
			// Removal of the channel mapping
			ChannelBrokerInfos chanInfos = channelsInfos.remove(channel);
			if (chanInfos == null) throw new UnknownChannelException("Channel "+channel+" doesn't exist (removed in parallel).");
			
			// If the channel creator was a client, and he is still registered on the broker : his number of remaining channels is increased
			String channelCreator = chanInfos.getCreator();
			if (channelCreator != null) {
				// Takes the channel creator lock
				ClientBrokerInfos clientChannelCreator = clientsInfos.get(channelCreator);
				synchronized(clientChannelCreator) {
					try {
						if (registeredService(channelCreator)) clientChannelCreator.increaseRemainingChannels();
						// Normally an exception would be throw if the creator is not FREE,
						// however here being the creator implies that he is not FREE, but privileged, so the exception in never actually thrown.
					} catch (ChannelQuotaExceededException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		// Dans le cahier des charges il est mentionné que les ports entrants offrant ReceivingCI doivent être déconnectés après 
		// destruction d'un channel, mais cela semble être trop pénalisant/pas naturel pour un tel système, du coup on fait le choix
		// de garder les ports connectés, mais via la destruction du mapping du channel d'implicitement unsubscribe les clients du channel, 
		// sans donc complètement tous les déconnecter du système
		
		return null;
	}
	
}
