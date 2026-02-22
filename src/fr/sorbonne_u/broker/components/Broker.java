package fr.sorbonne_u.broker.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import fr.sorbonne_u.broker.connectors.BrokerReceivingServiceConnector;
import fr.sorbonne_u.broker.ports.BrokerReceivingOutboundPort;
import fr.sorbonne_u.broker.ports.BrokerPrivilegedInboundPort;
import fr.sorbonne_u.broker.ports.BrokerPublishingInboundPort;
import fr.sorbonne_u.broker.ports.BrokerRegistrationInboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.cvm.AbstractCVM;
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

/**
 * Composant implantant un système de publication / souscription.
 * 
 * <p>
 * <li>Gère les enregistrements de composants clients (via l'URI de leur port entrant implantant {@link ReceivingCI}).
 * <li>Gère les canaux de transmission entre ces composants clients enregistrés.
 * <li>Gère les transmissions de messages en appliquant les filtres fournis par les composants abonnés au canal concerné.
 * <li>Gère les privilèges de clients non FREE, avec de meilleures performances et une gestion avancée des canaux.
 * </p>
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
	
	// Port entrant d'enregistrement du broker, obtenu par la méthode static 'registrationPortURI'
	private static final String registrationPortURI = "registration-inbound--port-broker";
	private BrokerRegistrationInboundPort registrationPort;
	public static String registrationPortURI() { return registrationPortURI;}
	
	// Port entrant de publication du broker, obtenu par l'enregistrement d'un port entrant de client
	private static final String publishingPortURI = "publishing-inbound-port-broker";
	private BrokerPublishingInboundPort publishingPort;
	
	// Port entrant de publication privilégiée du broker, obtenu par l'enregistrement d'un port entrant de client
	private static final String privilegedPortURI = "privileged-inbound-port-broker";
	private BrokerPrivilegedInboundPort privilegedPort;

	// Association : port entrant d'un client (URI) -> ses informations :
	// - sa RegistrationClass
	// - le port sortant vers CE client
	private Map<String, ClientBrokerInfos> clientsInfos = new HashMap<>(); 
	
	// Association : nom d'un channel -> ses informations :
	// - la RegistrationClass minimale acceptée par le channel
	// - un ensemble de paires : (port d'un client abonné + filtre sur le channel)
	// - le port client ayant créé le channel, ou null si c'est le Broker qui l'a créé au démarrage
	// - une liste de messages du channel
	private Map<String, ChannelBrokerInfos> channelsInfos = new HashMap<>();
	
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
		assert	c.isOfferedInterface(PrivilegedClientCI.class) :
			new InvariantException("The URI component should "
					+ "offer the interface PrivilegedClientCI!");
	}
	
	/**
	 * Crée un Broker avec 'N' channels.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code N>=0}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param N 					Le nombre de channels du Broker (de 'channel0' à 'channel[N-1]').
	 * @param nbMaxChannelsStd 		Le nombre maximal de channels pouvant être gérés par un client privilégié standard.
	 * @param nbMaxChannelsPrem 	Le nombre maximal de channels pouvant être gérés par un client privilégié premium.
	 * @throws Exception
	 */
	protected Broker(int N, int nbMaxChannelsStd, int nbMaxChannelsPrem) throws Exception {
		super(1, 0);
		assert(N>=0);
		
		ClientBrokerInfos.nbMaxChannelsStd = nbMaxChannelsStd;
		ClientBrokerInfos.nbMaxChannelsPrem = nbMaxChannelsPrem;
		
		// CREATION STATIQUE DE N CHANNELS
		for (int i = 0; i < N; i++) {
			String chan = "channel"+i;
			// DES CLIENTS FREE MINIMUM POUR LES CHANNELS PAR DEFAUT
			channelsInfos.put(chan, new ChannelBrokerInfos(chan, "*", null, false));
		}
		// channel0 -> données des vents des stations météo
		// channel1 -> alertes des bureaux météo

		this.registrationPort = new BrokerRegistrationInboundPort(registrationPortURI, this);
		this.registrationPort.publishPort();
		
		this.publishingPort = new BrokerPublishingInboundPort(publishingPortURI, this);
		this.publishingPort.publishPort();
		
		this.privilegedPort = new BrokerPrivilegedInboundPort(privilegedPortURI, this);
		this.privilegedPort.publishPort();

		Broker.checkInvariant(this) ;
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);

		if (AbstractCVM.isDistributed) {
			this.getLogger().setDirectory(System.getProperty("user.dir"));
		} else {
			this.getLogger().setDirectory(System.getProperty("user.home"));
		}
		this.getTracer().setTitle("Broker");
		this.getTracer().setRelativePosition(2, 2);
		
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
		
		assert	this.isPortExisting(privilegedPortURI) :
			new PostconditionException("The component must have a "
					+ "port with URI " + privilegedPortURI);
		assert	this.findPortFromURI(privilegedPortURI).
					getImplementedInterface().equals(PrivilegedClientCI.class) :
					new PostconditionException("The component must have a "
							+ "port with implemented interface PrivilegedClientCI");
		assert	this.findPortFromURI(privilegedPortURI).isPublished() :
					new PostconditionException("The component must have a "
							+ "port published with URI " + privilegedPortURI);
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
		for (Map.Entry<String, ClientBrokerInfos> entry : clientsInfos.entrySet()) {
			BrokerReceivingOutboundPort outboundPort = entry.getValue().getOutboundPort();
			this.doPortDisconnection(outboundPort.getPortURI());
		}
		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		
		try {
			// On unpublish tous les ports sortants liés aux clients enregistrés
			for (Map.Entry<String, ClientBrokerInfos> entry : clientsInfos.entrySet()) {
				BrokerReceivingOutboundPort outboundPort = entry.getValue().getOutboundPort();
				outboundPort.unpublishPort();
			}
		
			// On unpublish les 2 ports entrants
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
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		
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
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(rc != null);
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		
		return clientsInfos.containsKey(receptionPortURI) && clientsInfos.get(receptionPortURI).getRegistrationClass().equals(rc);
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
	 * @throws Exception                  If there is a problem while publishing the component's outbound port associated to the {@code receptionPortURI}.
	 * @throws Exception                  If there is a problem while connecting the new outbound port and {@code receptionPortURI}.
	 */
	public String registerService(String receptionPortURI, RegistrationClass rc) 
			throws AlreadyRegisteredException, Exception {
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(rc != null);
		
		if (registeredService(receptionPortURI)) throw new AlreadyRegisteredException("Client "+receptionPortURI+" already registered.");
		
		this.logMessage("Broker registering '" + receptionPortURI + "'") ;
		
		// On ajoute l'URI du port entrant enregistré dans la map l'associant à sa RegistrationClass et son port sortant du Broker
		BrokerReceivingOutboundPort outboundPort = new BrokerReceivingOutboundPort("BrokerOutboundPort-"+receptionPortURI, this);
		clientsInfos.put(receptionPortURI, new ClientBrokerInfos(receptionPortURI, rc, outboundPort));

		// On publie le nouveau port sortant
		outboundPort.localPublishPort() ;
		
		// On connecte le port sortant créé au port entrant donné
		this.doPortConnection(outboundPort.getPortURI(), receptionPortURI, BrokerReceivingServiceConnector.class.getCanonicalName());
		
		// Vérification des postconditions
		assert(registeredService(receptionPortURI));
		assert(registeredService(receptionPortURI, rc));
		
		if (rc == RegistrationClass.FREE) {
			return publishingPortURI;
		} else {
			return privilegedPortURI;
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
	 * @throws UnknownClientException       If the {@code receptionPortURI} is not registered.
	 * @throws AlreadyRegisteredException   If the client has already been registered.
	 */
	public String modifyServiceClassService(String receptionPortURI, RegistrationClass rc) 
			throws UnknownClientException, AlreadyRegisteredException {
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(rc != null);
		
		// L'appel peut lever une UnknownClientException si le client n'est pas déjà enregistré
		if (registeredService(receptionPortURI, rc) == true) throw new AlreadyRegisteredException("Client "+receptionPortURI+" already registered.");
		
		// On modifie la classe du client
		clientsInfos.get(receptionPortURI).setRegistrationClass(rc);
		
		// Gestion quand un client modifie sa classe alors qu'il est déjà subscribed à des channels
		// On parcourt toutes les paires : channel -> ses infos
		for (Map.Entry<String, ChannelBrokerInfos> entry : channelsInfos.entrySet()) {
			String channel = entry.getKey();
			ChannelBrokerInfos chanInfos = entry.getValue();
			
			// Si notre client est abonné, on regarde si le channel l'accepte avec sa nouvelle RegistrationClass
			if (chanInfos.isSubscribed(receptionPortURI)) {
				try {
					// Si ce n'est pas le cas, on le supprime en tant qu'abonné du channel
					if (!channelAuthorisedService(receptionPortURI, channel)) {
						chanInfos.removeSubscriber(receptionPortURI);
					}
				} catch (UnknownChannelException e) {
					System.err.println("Impossible, channel necessarily exists here");
				}
			}
		}

		// Vérification de la postcondition
		assert(registeredService(receptionPortURI, rc));
		
		if (rc == RegistrationClass.FREE) {
			return publishingPortURI;
		} else {
			return privilegedPortURI;
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
	 * @param receptionPortURI	               URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @throws UnknownClientException          If the {@code receptionPortURI} is not registered.
	 * @throws Exception                       If there is a problem while unpublishing the component's outbound port associated to the {@code receptionPortURI}.
	 * @throws Exception                       If there is a problem while disconnecting outbound port and {@code receptionPortURI}.
	 */
	public Void unregisterService(String receptionPortURI) 
			throws UnknownClientException, Exception {
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		
		this.logMessage("Broker unregistering '" + receptionPortURI +"'") ;
		
		BrokerReceivingOutboundPort outboundPort = clientsInfos.get(receptionPortURI).getOutboundPort();
		
		// On supprime les infos du port client
		clientsInfos.remove(receptionPortURI);

		// On déconnecte le port sortant du port entrant donné
		this.doPortDisconnection(outboundPort.getPortURI());
		
		// On unpublish le port sortant
		outboundPort.unpublishPort();
		
		// On supprime des channels abonnés le port précédemment enregistré
		for (Map.Entry<String, ChannelBrokerInfos> entry : channelsInfos.entrySet()) {
		    entry.getValue().removeSubscriber(receptionPortURI);
		}
		
		// Vérification de la postcondition
		assert(!registeredService(receptionPortURI));
		
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
		assert(channel != null && !channel.isEmpty());
		
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
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		
		if (clientsInfos.get(receptionPortURI).getRegistrationClass() == RegistrationClass.FREE
			&& channelsInfos.get(channel).onlyPrivileged()){
				return false;
			} else {
				return true;
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
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		
		return channelsInfos.get(channel).isSubscribed(receptionPortURI);
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
	 * @param receptionPortURI             URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           name of an existing channel.
	 * @param filter			           filter that accepts the message the component wants to receive.
	 * @throws UnknownClientException      If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException If {@code receptionPortURI} is not authorized to interact with the {@code channel}.
	 */
	public Void subscribeService(String receptionPortURI, String channel, MessageFilterI filter) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		assert(filter != null);

		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		if (!channelAuthorisedService(receptionPortURI, channel)) throw new UnauthorisedClientException("Client "+receptionPortURI+" is not authorized.");
		
		this.logMessage("Broker subscribing '" + receptionPortURI + "' to "+ channel) ;
		
		// On abonne le client au channel, avec le filtre associé
		channelsInfos.get(channel).addOrReplaceSubscriber(receptionPortURI, filter);
		
		// Vérification de la postcondition
		assert(subscribedService(receptionPortURI, channel));
		
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
	 * @param receptionPortURI	                 URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			                 name of an existing channel.
	 * @throws UnknownClientException		     If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException           If the {@code channel} doesn't exist.
	 * @throws NotSubscribedChannelException     If {@code receptionPortURI} is not subscribed to the {@code channel}.
	 */
	public Void unsubscribeService(String receptionPortURI, String channel) 
			throws Exception, UnknownChannelException, NotSubscribedChannelException {
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		if (!subscribedService(receptionPortURI, channel)) throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
		
		this.logMessage("Broker unsubscribing '" + receptionPortURI + "' from "+ channel) ;
		
		// On désabonne le client du channel
		channelsInfos.get(channel).removeSubscriber(receptionPortURI);
		
		// Vérification de la postcondition
		assert(!subscribedService(receptionPortURI, channel));
		
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
			throws Exception, UnknownChannelException, NotSubscribedChannelException {
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		assert(filter != null);
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		if (!subscribedService(receptionPortURI, channel)) throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
		
		// Ecrase le filtre existant par un nouveau
		channelsInfos.get(channel).addOrReplaceSubscriber(receptionPortURI, filter);
		
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
	 * @param receptionPortURI	           URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           name of the channel on which {@code message} must be published.
	 * @param messages			           list of messages to be published on {@code channel}. It can contain an only message.
	 * @throws UnknownClientException      If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException If {@code receptionPortURI} is not authorized to interact with the {@code channel}.
	 */
	public Void publishService(String receptionPortURI, String channel, ArrayList<MessageI> messages) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		assert(messages != null && messages.size() > 0);
		assert(Stream.of(messages).allMatch(m -> m != null));
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		if (!channelAuthorisedService(receptionPortURI, channel)) throw new UnauthorisedClientException("Client "+receptionPortURI+" is not authorized.");
		
		this.logMessage("Broker received message(s) from '" + receptionPortURI + "' on " + channel) ;
		
		ChannelBrokerInfos chanInfos = channelsInfos.get(channel);
		
		// On met les messages dans le channel
		chanInfos.addMessages(messages);
		
		// On envoie les messages aux clients concernés du channel
		MessageI msg = chanInfos.getMessage();
		while (msg != null) {
			
			final MessageI currentMsg = msg;
			
			// On récupère les clients du channel visés par ce message
			List<String> targetClientsPortURI = chanInfos.targetSubscribers(currentMsg);
			
			// On leur envoie le message
			for (String targetPortURI : targetClientsPortURI) {
				
				// Normalement impossible que la target ne soit pas enregistrée, simple vérification
				assert(registeredService(targetPortURI));
				
				this.logMessage("Broker sending message to '" + targetPortURI + "'") ;
				BrokerReceivingOutboundPort outboundPort = clientsInfos.get(targetPortURI).getOutboundPort();
				/*this.runTask(
						new AbstractComponent.AbstractTask() {
							@Override
							public void run() {
								try {
									outboundPort.receive(channel, currentMsg); // 1 message par envoi pour l'instant
								} catch (Throwable e) {
									e.printStackTrace();
								}
							}
						}) ;*/
				try {
					outboundPort.receive(channel, currentMsg); // 1 message par envoi pour l'instant
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			// Si le channel a été détruit, lancement d'une UnknownChannelException
			if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist anymore (destroyed).");
			msg = chanInfos.getMessage();
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
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		
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
		// Vérification de la précondition
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		
		return clientsInfos.get(receptionPortURI).canCreateChannel();
	}

	/**
	 * Create a new channel of the given name with a set of users authorised to use it.
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
	 * @param autorisedUsers						regular expression that matches the URIs of components that must be authorised to use the new channel.
	 * @throws UnknownClientException   			If the {@code receptionPortURI} is not registered.
	 * @throws AlreadyExistingChannelException   	If the {@code channel} already exists.
	 * @throws ChannelQuotaExceededException   		If the {@code receptionPortURI} cannot currently manage more channels.
	 */
	public Void createChannelService(String receptionPortURI, String channel, String autorisedUsers) 
			throws UnknownClientException,  AlreadyExistingChannelException, ChannelQuotaExceededException{
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		assert(autorisedUsers != null && !autorisedUsers.isEmpty());
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (channelsInfos.containsKey(channel)) throw new AlreadyExistingChannelException("Channel " + channel + " already exists.");
		if (!channelQuotaReachedService(receptionPortURI)) throw new ChannelQuotaExceededException("No remaining channels to manage for "+receptionPortURI);
		
		channelsInfos.put(channel, new ChannelBrokerInfos(channel, autorisedUsers, receptionPortURI, true));
		clientsInfos.get(receptionPortURI).decreaseRemainingChannels();
		return null;
	}

	/**
	 * Return true if {@code uri} is authorised to use {@code channel}.
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
	 * @return								true if {@code uri} is authorised to use {@code channel}.
	 * @throws UnknownClientException   	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @see Broker#channelAuthorisedService(String, String)
	 */
	public boolean isAuthorisedUserService(String channel, String uri) 
			throws UnknownClientException, UnknownChannelException {
		// Vérification des préconditions déléguée à l'appel à channelAuthorisedService
		// Cet appel permet de vérifier le fait que la RegistrationClass de l'uri soit compatible avec le channel.
		if (!channelAuthorisedService(uri, channel)) return false;
		
		// On récupère l'expression régulière acceptant les URIs des clients
		String authorised = channelsInfos.get(channel).getAuthorizedUsers();
				
		return uri.matches(authorised);
	}

	/**
	 * Replace the users authorised to use {@code channel}.
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
	 * @param autorisedUsers				regular expression that matches the URIs of components that must be authorised to use the new channel.
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not allowed to interact with the {@code channel}.
	 */
	public Void modifyAuthorisedUsersService(String receptionPortURI, String channel, String autorisedUsers)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		assert(autorisedUsers != null && !autorisedUsers.isEmpty());
		
		if (!isAuthorisedUserService(channel, receptionPortURI)) throw new UnauthorisedClientException("Client "+receptionPortURI+" not authorized");
		
		// Modifie le regex accepté, et délègue les éventuels désenregistrements à la méthode
		channelsInfos.get(channel).modifyAuthorizedUsers(autorisedUsers);
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
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not allowed to interact with the {@code channel}.
	 */
	public Void destroyChannelService(String receptionPortURI, String channel) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException{
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		if (!isAuthorisedUserService(channel, receptionPortURI)) throw new UnauthorisedClientException("Client "+receptionPortURI+" not authorized");
		
		// Suppression de la référence au channel
		ChannelBrokerInfos chanInfos = channelsInfos.remove(channel);
		assert(chanInfos != null);
		
		// Envoie les messages restants
		MessageI msg = chanInfos.getMessage();
		while (msg != null) {
			
			final MessageI currentMsg = msg;
			
			// On récupère les clients du channel visés par ce message
			List<String> targetClientsPortURI = chanInfos.targetSubscribers(currentMsg);
			
			// On leur envoie le message
			for (String targetPortURI : targetClientsPortURI) {
				
				// Normalement impossible que la target ne soit pas enregistrée, simple vérification
				assert(registeredService(targetPortURI));
				
				this.logMessage("Broker sending message to '" + targetPortURI + "' after channel destruction") ;
				BrokerReceivingOutboundPort outboundPort = clientsInfos.get(targetPortURI).getOutboundPort();
				try {
					outboundPort.receive(channel, currentMsg); // 1 message par envoi pour l'instant
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			msg = chanInfos.getMessage();
		}
		
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
	 * @throws UnknownClientException      	If the {@code receptionPortURI} is not registered.
	 * @throws UnknownChannelException     	If the {@code channel} doesn't exist.
	 * @throws UnauthorisedClientException  If the {@code receptionPortURI} is not allowed to interact with the {@code channel}.
	 */
	public Void destroyChannelNowService(String receptionPortURI, String channel) 
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException{
		// Vérification des préconditions
		assert(receptionPortURI != null && !receptionPortURI.isEmpty());
		assert(channel != null && !channel.isEmpty());
		
		if (!registeredService(receptionPortURI)) throw new UnknownClientException("Client "+receptionPortURI+" not registered.");
		if (!channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist.");
		if (!isAuthorisedUserService(channel, receptionPortURI)) throw new UnauthorisedClientException("Client "+receptionPortURI+" not authorized");
		
		// Suppression de la référence au channel
		ChannelBrokerInfos chanInfos = channelsInfos.remove(channel);
		assert(chanInfos != null);
		
		return null;
	}
	
}
