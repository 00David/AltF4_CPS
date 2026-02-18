package fr.sorbonne_u.broker.ports;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;

/**
 * Port entrant d'un Broker, permettant de gérer les publications des messages d'un client privilégié, et sa gestion avancée des canaux.<br>
 * Délègue la gestion à son possesseur, un composant {@link Broker}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerPrivilegedInboundPort
extends		AbstractInboundPort
implements	PrivilegedClientCI {

	private static final long serialVersionUID = 1878031196993861332L;

	/**
	 * Crée le port entrant avec l'{@code uri} fournie, dont le composant posseseur {@code owner} est un {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	uri != null and owner != null
	 * pre	owner instanceof {@link Broker}
	 * post	true			// pas de postcondition.
	 * </pre>
	 * 
	 * @param uri          l'URI du port.
	 * @param owner        Le composant possesseur du port.
	 */
	public BrokerPrivilegedInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, PrivilegedClientCI.class, owner) ;
		// Vérification des préconditions
		assert(uri != null);
		assert(owner != null);
		assert(owner instanceof Broker);
	}
	
	// Toutes les préconditions et postconditions sont vérifiées par délégation, lors de l'exécution du service du Broker appellé.

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		ArrayList<MessageI> messages =  new ArrayList<MessageI>();
		messages.add(message);
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).publishService(receptionPortURI, channel, messages));
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).publishService(receptionPortURI, channel, messages));
	}

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).hasCreatedChannelService(receptionPortURI, channel));
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).channelQuotaReachedService(receptionPortURI));
	}

	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).createChannelService(receptionPortURI, channel, autorisedUsers));
	}

	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).isAuthorisedUserService(channel, uri));
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyAuthorisedUsersService(receptionPortURI, channel, autorisedUsers));
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression) throws Exception  {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyAuthorisedUsersService(receptionPortURI, channel, regularExpression)); // ?
		
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).destroyChannelService(receptionPortURI, channel));
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).destroyChannelNowService(receptionPortURI, channel));
	}
	
}
