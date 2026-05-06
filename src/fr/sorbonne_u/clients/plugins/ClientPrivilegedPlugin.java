package fr.sorbonne_u.clients.plugins;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.clients.connectors.PrivilegedServiceConnector;
import fr.sorbonne_u.clients.ports.PrivilegedOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The {@link Client} plug-in having a {@link PrivilegedOutboundPort}.
 * 
 * <p>
 * This plug-in needs to be created after registering on the {@link Broker}, in order to achieve 
 * a valid connection to an inbound {@link Broker} privileged port.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code brokerPublishingPortURI != null && !brokerPublishingPortURI.isEmpty()}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientPrivilegedPlugin extends AbstractPlugin implements PrivilegedClientI {

	private static final long serialVersionUID = -1472841763925529259L;
	
	/** The {@link Broker} inbound port URI offering {@link PrivilegedClientCI} interface. */
	protected final String brokerPublishingPortURI;
	
	// PRIVILEGED MANAGEMENT

	/** Privileged outbound port, set up after plugin installation + initialization */
	protected PrivilegedOutboundPort privilegedOutboundPort;
	
	/**
	 * Creates a privileged {@link Client} plug-in.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code brokerPublishingPortURI != null && !brokerPublishingPortURI.isEmpty()}
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @param brokerPublishingPortURI		The {@link Broker} inbound port URI offering {@link PrivilegedClientCI} interface.
	 */
	public ClientPrivilegedPlugin(String brokerPublishingPortURI){
		// Precondition check
		assert(brokerPublishingPortURI != null && !brokerPublishingPortURI.isEmpty()) : 
			new PreconditionException("brokerPublishingPortURI cannot be null or empty");
		
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
		super.initialise();
		this.privilegedOutboundPort = new PrivilegedOutboundPort(this.getOwner());
		this.privilegedOutboundPort.publishPort();
		
		this.getOwner().doPortConnection(this.privilegedOutboundPort.getPortURI(),
				brokerPublishingPortURI,
				PrivilegedServiceConnector.class.getCanonicalName());
	}
	
	@Override
	public void	finalise() throws Exception {
		this.getOwner().doPortDisconnection(this.privilegedOutboundPort.getPortURI());
		super.finalise();
	}
	
	@Override
	public void uninstall() throws Exception {
		this.privilegedOutboundPort.unpublishPort();
		this.removeRequiredInterface(PrivilegedClientCI.class);
		super.uninstall();
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------

	// Precondition and postcondition verifications are delegated to the port used

	@Override
	public boolean hasCreatedChannel(String channel) throws UnknownClientException, UnknownChannelException {
		Client owner = (Client)this.getOwner();
		try {
			return privilegedOutboundPort.hasCreatedChannel(owner.receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean channelQuotaReached() throws UnknownClientException {
		Client owner = (Client)this.getOwner();
		try {
			return privilegedOutboundPort.channelQuotaReached(owner.receivingInboundPortURI);
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
		Client owner = (Client)this.getOwner();
		try {
			privilegedOutboundPort.createChannel(owner.receivingInboundPortURI, channel, autorisedUsers);
		} catch (UnknownClientException | AlreadyExistingChannelException | ChannelQuotaExceededException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override // deleted from other interfaces, so unused
	public boolean isAuthorisedUser(String channel, String uri) throws UnknownClientException, UnknownChannelException {
		return true;
	}

	@Override
	public void modifyAuthorisedUsers(String channel, String autorisedUsers)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		Client owner = (Client)this.getOwner();
		try {
			privilegedOutboundPort.modifyAuthorisedUsers(owner.receivingInboundPortURI, channel, autorisedUsers);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void destroyChannel(String channel)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		Client owner = (Client)this.getOwner();
		try {
			privilegedOutboundPort.destroyChannel(owner.receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void destroyChannelNow(String channel)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		Client owner = (Client)this.getOwner();
		try {
			privilegedOutboundPort.destroyChannelNow(owner.receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
