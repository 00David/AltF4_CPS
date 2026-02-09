package fr.sorbonne_u.broker.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fr.sorbonne_u.broker.connectors.BrokerReceivingServiceConnector;
import fr.sorbonne_u.broker.ports.BrokerReceivingOutboundPort;
import fr.sorbonne_u.broker.ports.BrokerPublishingInboundPort;
import fr.sorbonne_u.broker.ports.BrokerRegistrationInboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;

/**
 * Composant implantant un système de publication / souscription.
 * 
 * <p>
 * <li>Gère les enregistrements de composants clients (via l'URI de leur port entrant implantant {@link ReceivingCI}).
 * <li>Gère les canaux de transmission entre ces composants clients enregistrés.
 * <li>Gère les transmissions de messages en appliquant les filtres fournis par les composants abonnés au canal concerné.
 * </p>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
@OfferedInterfaces(offered = {RegistrationCI.class, PublishingCI.class}) // PrivilegedClientCI pour plus tard
@RequiredInterfaces(required = {ReceivingCI.class})
public class Broker extends AbstractComponent{
	
	//--------------------------------------------------------------------------
	// Internal datas
	//--------------------------------------------------------------------------
	
	// Port entrant d'enregistrement du broker, obtenu par la méthode static 'registrationPortURI'
	private static final String registrationPortURI = "registration-inbound--port-broker";
	private BrokerRegistrationInboundPort registrationPort;
	public static String registrationPortURI() { return registrationPortURI;}
	
	// Port entrant de publication du broker, obtenu par l'enregistrement d'un port entrant de client
	private static final String publishingPortURI = "publishing-inbound-port-broker";
	private BrokerPublishingInboundPort publishingPort;
	
	// Utilisé pour un channel : paire port d'un client abonné + filtre sur le channel
	// La paire n'est identifiée QUE par le port du client
	private static final class ClientChannelFilter {
	    private final String clientPortURI;
	    private final MessageFilterI filter;

	    private ClientChannelFilter(String clientPortURI,
	                                 MessageFilterI filter) {
	        this.clientPortURI = clientPortURI;
	        this.filter = filter;
	    }
	    
	    @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (o == null || getClass() != o.getClass()) return false;

	        ClientChannelFilter that = (ClientChannelFilter) o;
	        return clientPortURI.equals(that.clientPortURI);
	    }

	    @Override
	    public int hashCode() { return clientPortURI.hashCode(); }
	}

	// Association : port entrant d'un client (URI) -> sa RegistrationClass 
	private Map<String, RegistrationClass> registeredReceptionPorts = new HashMap<>(); 
	
	// Association : port entrant d'un client (URI) -> port sortant vers CE client
	private Map<String, BrokerReceivingOutboundPort> registeredDeliveryPorts = new HashMap<>(); 
	
	// Association : nom d'un channel -> un ensemble de ports entrants (URIs) de clients abonnés au channel
	private Map<String, Set<ClientChannelFilter>> channels = new HashMap<>();
	
	// Association : nom d'un channel -> une liste de messages du channel
	private Map<String, List<MessageI>> channelsMessages = new HashMap<>();
	
	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	protected static void	checkInvariant(Broker c) {
		assert	c.isOfferedInterface(RegistrationCI.class) :
					new InvariantException("The URI component should "
							+ "offer the interface RegistrationCI!");
		assert	c.isOfferedInterface(PublishingCI.class) :
			new InvariantException("The URI component should "
					+ "offer the interface PublishingCI!");
	}
	
	protected Broker() throws Exception {
		super(1, 0);
		
		// CREATION STATIQUE DE 2 CHANNELS
		channels.put("Channel1", new HashSet<>());
		channelsMessages.put("Channel1", new ArrayList<>());
		channels.put("Channel2", new HashSet<>());
		channelsMessages.put("Channel2", new ArrayList<>());

		this.registrationPort = new BrokerRegistrationInboundPort(registrationPortURI, this);
		this.registrationPort.publishPort();
		
		this.publishingPort = new BrokerPublishingInboundPort(publishingPortURI, this);
		this.publishingPort.publishPort();

		Broker.checkInvariant(this) ;
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);

		if (AbstractCVM.isDistributed) {
			this.getLogger().setDirectory(System.getProperty("user.dir"));
		} else {
			this.getLogger().setDirectory(System.getProperty("user.home"));
		}
		this.getTracer().setTitle("Broker");
		this.getTracer().setRelativePosition(1, 0);
		
		assert	this.isPortExisting(registrationPortURI) :
					new PostconditionException("The component must have a "
							+ "port with URI " + registrationPortURI);
		assert	this.findPortFromURI(registrationPortURI).
					getImplementedInterface().equals(RegistrationCI.class) :
					new PostconditionException("The component must have a "
							+ "port with implemented interface RegistrationCI");
		assert	this.findPortFromURI(registrationPortURI).isPublished() :
					new PostconditionException("The component must have a "
							+ "port published with URI " + registrationPortURI);
		
		assert	this.isPortExisting(publishingPortURI) :
			new PostconditionException("The component must have a "
					+ "port with URI " + publishingPortURI);
		assert	this.findPortFromURI(publishingPortURI).
					getImplementedInterface().equals(PublishingCI.class) :
					new PostconditionException("The component must have a "
							+ "port with implemented interface RegistrationCI");
		assert	this.findPortFromURI(publishingPortURI).isPublished() :
					new PostconditionException("The component must have a "
							+ "port published with URI " + publishingPortURI);
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
		
		// On déconnecte tous les ports sortants liés aux clients enregistrés
		for (Map.Entry<String, BrokerReceivingOutboundPort> entry : registeredDeliveryPorts.entrySet()) {
			BrokerReceivingOutboundPort outboundPort = entry.getValue();
			this.doPortDisconnection(outboundPort.getPortURI());
		}
		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		
		try {
			// On unpublish tous les ports sortants liés aux clients enregistrés
			for (Map.Entry<String, BrokerReceivingOutboundPort> entry : registeredDeliveryPorts.entrySet()) {
				BrokerReceivingOutboundPort outboundPort = entry.getValue();
				outboundPort.unpublishPort();
				
			}
		
			// On unpublish les 2 ports entrants
			/*PortI[] p;
			p = this.findPortsFromInterface(RegistrationCI.class);
			p[0].unpublishPort();
	
			p = this.findPortsFromInterface(PublishingCI.class);
			p[0].unpublishPort();*/
			this.registrationPort.unpublishPort();
			this.publishingPort.unpublishPort();
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
	 * Return true if {@code receptionPortURI} has already been registered with service class {@code rc}, otherwise false.<br>
	 * If there is no {@link RegistrationClass} given, it is not taken into account.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				the registration class. Can be {@code null}.
	 * @return					true if {@code receptionPortURI} has already been registered, with service class {@code rc} if given, otherwise false.
	 */
	public boolean registeredService(String receptionPortURI, RegistrationClass rc){
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		
		if (rc == null) {
			return registeredReceptionPorts.containsKey(receptionPortURI);
		} else {
			return registeredReceptionPorts.containsKey(receptionPortURI) && registeredReceptionPorts.containsValue(rc);
		}
	}

	/**
	 * Register on the publication/subscription system passing the URI of the inbound port offering the component interface {@code ReceivingCI}.<br>
	 * Returns an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code registeredService(receptionPortURI, null)}
	 * </pre>
	 *
	 * @param receptionPortURI	          URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				          the required registration class.
	 * @return					          an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * @throws AlreadyRegisteredException If the client has already been registered.
	 * @throws Exception                  If there is a problem while publishing the component's outbound port associated to the {@code receptionPortURI}.
	 * @throws Exception                  If there is a problem while connecting the new outbound port and {@code receptionPortURI}.
	 */
	public String registerService(String receptionPortURI, RegistrationClass rc) 
			throws AlreadyRegisteredException, Exception {
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		assert(rc != null);
		
		if (registeredService(receptionPortURI, null)) throw new AlreadyRegisteredException("Client already registered.");
		
		this.logMessage("Broker registering " + receptionPortURI) ;
		
		// On ajoute l'URI du port entrant enregistré dans la map l'associant à sa RegistrationClass
		registeredReceptionPorts.put(receptionPortURI, rc);
		
		// On ajoute l'URI du port entrant dans la map l'associant au nouveau port sortant associé du Broker
		BrokerReceivingOutboundPort outboundPort = new BrokerReceivingOutboundPort("BrokerOutboundPort-"+receptionPortURI, this);
		registeredDeliveryPorts.put(receptionPortURI, outboundPort);
		outboundPort.localPublishPort() ;
		
		// On connecte le port sortant créé au port entrant donné
		this.doPortConnection(outboundPort.getPortURI(), receptionPortURI, BrokerReceivingServiceConnector.class.getCanonicalName());
		
		// Vérification de la postcondition
		assert(registeredService(receptionPortURI, null));
		
		return publishingPortURI;
	}

	/**
	 * Upgrade or downgrade the registration to the class {@code rc}.<br>
	 * Returns a new URI of an inbound port offering the component interface {@code PublishingCI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * pre	{@code registeredService(receptionPortURI, null)}
	 * pre	{@code !registeredService(receptionPortURI, rc)}
	 * post	{@code registeredService(receptionPortURI, rc)}
	 * </pre>
	 *
	 * @param receptionPortURI	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				the required registration class.
	 * @return					an URI of an inbound port offering the component interface {@code PublishingCI}.
	 */
	public String modifyServiceClassService(String receptionPortURI, RegistrationClass rc) {
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		assert(rc != null);
		assert(registeredService(receptionPortURI, null));
		assert(!registeredService(receptionPortURI, rc));
		
		registeredReceptionPorts.put(receptionPortURI, rc);

		// Vérification de la postcondition
		assert(registeredService(receptionPortURI, rc));
		
		return publishingPortURI;
	}

	/**
	 * Unregister from the publication/subscription system.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	               URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @throws UnknownIdentifierException      If the {@code receptionPortURI} is not registered.
	 * @throws Exception                       If there is a problem while unpublishing the component's outbound port associated to the {@code receptionPortURI}.
	 * @throws Exception                       If there is a problem while disconnecting outbound port and {@code receptionPortURI}.
	 */
	public Void unregisterService(String receptionPortURI) 
			throws UnknownClientException, Exception {
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		
		if (!registeredService(receptionPortURI, null)) throw new UnknownClientException("Client not registered.");
		
		this.logMessage("Broker unregistering " + receptionPortURI) ;
		
		// On supprime des ports enregistrés
		registeredReceptionPorts.remove(receptionPortURI);
		
		// On supprime de la map l'association avec un port sortant du Broker
		BrokerReceivingOutboundPort outboundPort = registeredDeliveryPorts.remove(receptionPortURI);

		// On déconnecte le port sortant du port entrant donné
		this.doPortDisconnection(outboundPort.getPortURI());
		
		// On unpublish le port sortant
		outboundPort.unpublishPort();

		
		// On supprime des channels abonnés le port précédemment enregistré
		for (Map.Entry<String, Set<ClientChannelFilter>> entry : channels.entrySet()) {
		    entry.getValue().remove(new ClientChannelFilter(receptionPortURI, null));
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
		// Vérification des préconditions
		assert(channel != null);
		assert(!channel.isEmpty());
		
		return channels.containsKey(channel);
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
	 * @throws UnknownIdentifierException  If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     If the {@code channel} doesn't exist.
	 */
	public boolean subscribedService(String receptionPortURI, String channel) 
			throws Exception, UnknownChannelException {
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		assert(channel != null);
		assert(!channel.isEmpty());
		
		if (!registeredService(receptionPortURI, null)) throw new UnknownClientException("Client not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		
		return channels.get(channel).contains(new ClientChannelFilter(receptionPortURI, null));
	}

	/**
	 * Subscribe to {@code channel} with filter {@code filter}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI             URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           name of an existing channel.
	 * @param filter			           filter that accepts the message the component wants to receive.
	 * @throws UnknownIdentifierException  If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     If the {@code channel} doesn't exist.
	 */
	public Void subscribeService(String receptionPortURI, String channel, MessageFilterI filter) 
			throws UnknownClientException, UnknownChannelException {
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		assert(channel != null);
		assert(!channel.isEmpty());
		assert(filter != null);
		
		if (!registeredService(receptionPortURI, null)) throw new UnknownClientException("Client not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		
		this.logMessage("Broker subscribing " + receptionPortURI + " to channel "+ channel) ;
		
		channels.get(channel).add(new ClientChannelFilter(receptionPortURI, filter));
		return null;
	}

	/**
	 * Unsubscribe from {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	                 URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			                 name of an existing channel.
	 * @throws UnknownIdentifierException		 If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException           If the {@code channel} doesn't exist.
	 * @throws NotSubscribedChannelException     If {@code receptionPortURI} is not subscribed to the {@code channel}.
	 */
	public Void unsubscribeService(String receptionPortURI, String channel) 
			throws Exception, UnknownChannelException, NotSubscribedChannelException {
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		assert(channel != null);
		assert(!channel.isEmpty());
		
		if (!registeredService(receptionPortURI, null)) throw new UnknownClientException("Client not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		if (!subscribedService(receptionPortURI, channel)) throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
		
		this.logMessage("Broker unsubscribing " + receptionPortURI + " from channel "+ channel) ;
		
		channels.get(channel).remove(new ClientChannelFilter(receptionPortURI, null));
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
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	                 URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel		                 	 name of an existing channel.
	 * @param filter			                 filter that accepts the message the component wants to receive.
	 * @return					                 true if the modification has been performed, otherwise false.
	 * @throws UnknownIdentifierException		 If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException           If the {@code channel} doesn't exist.
	 * @throws NotSubscribedChannelException     If {@code receptionPortURI} is not subscribed to the {@code channel}.
	 */
	public boolean modifyFilterService(String receptionPortURI, String channel, MessageFilterI filter) 
			throws Exception, UnknownChannelException, NotSubscribedChannelException {
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		assert(channel != null);
		assert(!channel.isEmpty());
		
		if (!registeredService(receptionPortURI, null)) throw new UnknownClientException("Client not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		if (!subscribedService(receptionPortURI, channel)) throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
		
		channels.get(channel).add(new ClientChannelFilter(receptionPortURI, filter));
		return true;
	}
	
	// PUBLICATIONS
	
	/**
	 * Publish all of the {@code messages} on an existing {@code channel} from a registered {@code receptionPortURI}.<br>
	 * For now, also send those {@code messages} to clients accepting them after applying their filters.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.size() > 0}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	           URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           name of the channel on which {@code message} must be published.
	 * @param messages			           list of messages to be published on {@code channel}. It can contain an only message.
	 * @throws UnknownIdentifierException  If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     If the {@code channel} doesn't exist.
	 */
	public Void publishService(String receptionPortURI, String channel, ArrayList<MessageI> messages) 
			throws UnknownClientException, UnknownChannelException {
		// Vérification des préconditions
		assert(receptionPortURI != null);
		assert(!receptionPortURI.isEmpty());
		assert(channel != null);
		assert(!channel.isEmpty());
		assert(messages != null);
		assert(messages.size() > 0);
		
		if (!registeredService(receptionPortURI, null)) throw new UnknownClientException("Client not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		
		this.logMessage("Broker received messages from " + receptionPortURI) ;
		
		// On met les messages dans le channel
		channelsMessages.get(channel).addAll(messages);
		
		// On envoie les messages aux clients concernés du channel
		for (ClientChannelFilter ccf : channels.get(channel)) {
			
			for(MessageI msg : messages) {
				if (ccf.filter.match(msg)){
					this.logMessage("Broker sending message to " + ccf.clientPortURI) ;
					BrokerReceivingOutboundPort outboundPort = registeredDeliveryPorts.get(ccf.clientPortURI);
					this.runTask(
							new AbstractComponent.AbstractTask() {
								@Override
								public void run() {
									try {
										outboundPort.receive(channel, msg); // 1 message par envoi pour l'instant
									} catch (Throwable e) {
										e.printStackTrace();
									}
								}
							}) ;
				}
			}
		}
		
		return null;
	}
	
}
