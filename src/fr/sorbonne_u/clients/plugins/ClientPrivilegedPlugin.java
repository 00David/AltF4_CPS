package fr.sorbonne_u.clients.plugins;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPrivilegedServiceConnector;
import fr.sorbonne_u.clients.ports.ClientPrivilegedOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The {@link Client} plugin having a {@link ClientPrivilegedOutboundPort}.
 * 
 * <p>
 * This plugin needs to be created after registering on the {@link Broker}, in order to pass 
 * a valid {@code brokerPublishingPortURI} argument to the constructor.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientPrivilegedPlugin extends AbstractPlugin implements PrivilegedClientI {

	private static final long serialVersionUID = -1472841763925529259L;
	
	// RECEPTION
	
	/** {@link Client} reception inbound port URI */
	protected final String receivingInboundPortURI;
	
	// PRIVILEGED MANAGEMENT
	
	/** {@link Client} privileged outbound port URI */
	protected final String privilegedOutboundPortURI;

	/** Privileged outbound port, set up after plugin installation + initialization */
	protected ClientPrivilegedOutboundPort privilegedOutboundPort;
	
	// PUBLICATION
	
	/** {@link Broker} publication inbound port URI, got via {@link ClientRegistrationPlugin} after registration */
	protected final String brokerPublishingPortURI;
	
	/**
	 * Creates a privileged {@link Client} plugin.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code privilegedOutboundPortURI != null && !privilegedOutboundPortURI.isEmpty()}
	 * pre  {@code brokerPublishingPortURI != null && !brokerPublishingPortURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI		the client inbound port URI offering {@link ReceivingCI} interface.
	 * @param privilegedOutboundPortURI		the client outbound port URI requiring {@link PrivilegedClientCI} interface, used for creating the {@link ClientPrivilegedOutboundPort}.
	 * @param brokerPublishingPortURI		the {@link Broker} inbound port URI offering {@link PrivilegedClientCI} interface.
	 */
	public ClientPrivilegedPlugin(String receivingInboundPortURI, String privilegedOutboundPortURI, String brokerPublishingPortURI){
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()) : new PreconditionException("receivingInboundPortURI cannot be null or empty");
		assert(privilegedOutboundPortURI != null && !privilegedOutboundPortURI.isEmpty()) : new PreconditionException("privilegedOutboundPortURI cannot be null or empty");
		assert(brokerPublishingPortURI != null && !brokerPublishingPortURI.isEmpty()) : new PreconditionException("brokerPublishingPortURI cannot be null or empty");
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.privilegedOutboundPortURI = privilegedOutboundPortURI;
		this.brokerPublishingPortURI = brokerPublishingPortURI;
	}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addRequiredInterface(PrivilegedClientCI.class);
	}
	
	@Override
	public void	initialise() throws Exception {
		this.privilegedOutboundPort = new ClientPrivilegedOutboundPort(privilegedOutboundPortURI, this.getOwner());
		this.privilegedOutboundPort.publishPort();
		
		this.getOwner().doPortConnection(this.privilegedOutboundPort.getPortURI(),
				brokerPublishingPortURI,
				ClientPrivilegedServiceConnector.class.getCanonicalName());
		super.initialise();
	}
	
	@Override
	public void	finalise() throws Exception {
		this.getOwner().doPortDisconnection(this.privilegedOutboundPort.getPortURI());
	}
	
	@Override
	public void uninstall() throws Exception {
		this.privilegedOutboundPort.unpublishPort();
		this.removeRequiredInterface(PrivilegedClientCI.class);
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------

	// Precondition and postcondition verifications are delegated to the port used

	@Override
	public boolean hasCreatedChannel(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return privilegedOutboundPort.hasCreatedChannel(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean channelQuotaReached() throws UnknownClientException {
		try {
			return privilegedOutboundPort.channelQuotaReached(receivingInboundPortURI);
		} catch (UnknownClientException  e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void createChannel(String channel, String autorisedUsers)
			throws UnknownClientException, AlreadyExistingChannelException, ChannelQuotaExceededException {
		try {
			privilegedOutboundPort.createChannel(receivingInboundPortURI, channel, autorisedUsers);
		} catch (UnknownClientException | AlreadyExistingChannelException | ChannelQuotaExceededException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws UnknownClientException, UnknownChannelException {
		try {
			return privilegedOutboundPort.isAuthorisedUser(channel, uri);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void modifyAuthorisedUsers(String channel, String autorisedUsers)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			privilegedOutboundPort.modifyAuthorisedUsers(receivingInboundPortURI, channel, autorisedUsers);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void destroyChannel(String channel)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			privilegedOutboundPort.destroyChannel(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void destroyChannelNow(String channel)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			privilegedOutboundPort.destroyChannelNow(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
