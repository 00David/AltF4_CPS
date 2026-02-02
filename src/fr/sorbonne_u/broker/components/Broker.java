package fr.sorbonne_u.broker.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fr.sorbonne_u.broker.connectors.BrokerPublishingInboundPort;
import fr.sorbonne_u.broker.connectors.BrokerRegistrationInboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.examples.basic_cs.interfaces.URIProviderCI;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.ports.PortI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.filters.MessageFilter;

/**
 * @author Kevin Tang
 * @author David Vadimon
 */
@OfferedInterfaces(offered = {RegistrationCI.class, PublishingCI.class}) // PrivilegedClientCI pour plus tard
public class Broker extends AbstractComponent{
	
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
	
	private static final String registrationPortURI = "registration-port-broker";
	private static final String publishingPortURI = "publishing-port-broker";

	public static String registrationPortURI() { return registrationPortURI;}
	
	private Map<String, RegistrationClass> registeredReceptionPorts = new HashMap<>(); // URIs
	
	private Map<String, Set<ClientChannelFilter>> channels = new HashMap<>(); // nom d'un channel-> un ensemble de clients (URIs) abonnés
	private Map<String, List<MessageI>> channelsMessages = new HashMap<>(); // nom d'un channel-> une liste de messages
	
	
	protected static void	checkInvariant(Broker c)
	{
		assert	c.isOfferedInterface(RegistrationCI.class) :
					new InvariantException("The URI component should "
							+ "offer the interface RegistrationCI!");
		assert	c.isOfferedInterface(PublishingCI.class) :
			new InvariantException("The URI component should "
					+ "offer the interface PublishingCI!");
	}
	
	protected Broker(String registrationPortURI, String publishingPortURI) throws Exception {
		super(1, 0);

		PortI pRegistration = new BrokerRegistrationInboundPort(registrationPortURI, this);
		pRegistration.publishPort();
		
		PortI pPublishing = new BrokerPublishingInboundPort(publishingPortURI, this);
		pPublishing.publishPort();

		Broker.checkInvariant(this) ;
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);

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
	public void			start() throws ComponentStartException
	{
		this.logMessage("starting Broker component.");
		super.start();
	}

	@Override
	public void			finalise() throws Exception
	{
		this.logMessage("stopping Broker component.");
		this.printExecutionLogOnFile("Broker");
		super.finalise();
	}

	@Override
	public void			shutdown() throws ComponentShutdownException
	{
		try {
			PortI[] p = this.findPortsFromInterface(URIProviderCI.class);
			p[0].unpublishPort();
			p[1].unpublishPort();
		} catch (Throwable e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	@Override
	public void			shutdownNow() throws ComponentShutdownException
	{
		try {
			PortI[] p = this.findPortsFromInterface(URIProviderCI.class);
			p[0].unpublishPort();
			p[1].unpublishPort();
		} catch (Throwable e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}
	
	//--------------------------------------------------------------------------
	// Component internal services
	//--------------------------------------------------------------------------
	
	// Registrations
	
	public boolean registered(String receptionPortURI) throws Exception {
		return registeredReceptionPorts.containsKey(receptionPortURI);
	}

	public boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception {
		return registeredReceptionPorts.containsKey(receptionPortURI) && registeredReceptionPorts.containsValue(rc);
	}

	public String register(String receptionPortURI, RegistrationClass rc) throws Exception {
		if (registered(receptionPortURI)) throw new AlreadyRegisteredException("Client already registered.");
		
		registeredReceptionPorts.put(receptionPortURI, rc);
		return publishingPortURI;
	}

	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc) throws Exception {
		if (!registered(receptionPortURI)) throw new Exception("Client not registered.");
		
		registeredReceptionPorts.put(receptionPortURI, rc);
		return publishingPortURI;
	}

	public Void unregister(String receptionPortURI) throws Exception {
		if (!registered(receptionPortURI)) throw new Exception("Client not registered.");
		
		// on supprime des ports enregistrés
		registeredReceptionPorts.remove(receptionPortURI);
		
		// on supprime des channels abonnés
		for (Map.Entry<String, Set<ClientChannelFilter>> entry : channels.entrySet()) {
		    entry.getValue().remove(new ClientChannelFilter(receptionPortURI, null));
		}
		
		return null;
	}

	public boolean channelExists(String channel) throws Exception {
		return channels.containsKey(channel);
	}

	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		if (!registered(receptionPortURI)) throw new Exception("Client not registered.");
		if (!channelExists(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		
		return channels.get(channel).contains(new ClientChannelFilter(receptionPortURI, null));
	}

	public Void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		if (!registered(receptionPortURI)) throw new Exception("Client not registered.");
		if (!channelExists(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		
		channels.get(channel).add(new ClientChannelFilter(receptionPortURI, filter));
		return null;
	}

	public Void unsubscribe(String receptionPortURI, String channel) throws Exception {
		if (!registered(receptionPortURI)) throw new Exception("Client not registered.");
		if (!subscribed(receptionPortURI, channel)) throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
		
		channels.get(channel).remove(new ClientChannelFilter(receptionPortURI, null));
		return null;
	}

	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		if (!registered(receptionPortURI)) throw new Exception("Client not registered.");
		if (!subscribed(receptionPortURI, channel)) throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
		
		channels.get(channel).add(new ClientChannelFilter(receptionPortURI, filter));
		return true;
	}
	
	// Publications
	
	public Void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		if (!registered(receptionPortURI)) throw new Exception("Client not registered.");
		if (!channelExists(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		
		channelsMessages.get(receptionPortURI).add(message);
		return null;
	}
	
	public Void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		if (!registered(receptionPortURI)) throw new Exception("Client not registered.");
		if (!channelExists(channel)) throw new UnknownChannelException("Channel doesn't exist.");
		
		channelsMessages.get(receptionPortURI).addAll(messages);
		return null;
	}
	
}
