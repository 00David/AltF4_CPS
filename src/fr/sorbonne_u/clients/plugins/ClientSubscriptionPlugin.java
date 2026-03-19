package fr.sorbonne_u.clients.plugins;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.ports.ClientReceivingPluginInboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientSubscriptionI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The {@link Client} plugin having a {@link ClientReceivingPluginInboundPort}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
 * invariant	{@code nbMessagesReceived != null && nbMessagesReceived.get() >= 0}
 * invariant	{@code registrationOutboundPort != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientSubscriptionPlugin extends AbstractPlugin implements ClientSubscriptionI {

	private static final long serialVersionUID = -1472841763925529259L;
	
	// RECEPTION
	
	/** {@link Client} reception inbound port URI */
	protected final String receivingInboundPortURI;
	
	/** Reception inbound port, set up after plugin installation + initialization */
	protected ClientReceivingPluginInboundPort receivingInboundPort;
	
	/** A counter incremented on each message received. Used for testing. */
	protected AtomicInteger nbMessagesReceived = new AtomicInteger();
	
	// REGISTRATION
	
	/** Registration outbound port, owned by {@link ClientRegistrationPlugin} */
	protected final ClientRegistrationOutboundPort registrationOutboundPort;
	
	/**
	 * Creates a {@link Client} subscription + reception plugin.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationOutboundPort != null}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI		the client inbound port URI offering {@link ReceivingCI} interface, used for creating the {@link ClientReceivingPluginInboundPort}.
	 * @param registrationOutboundPort		the client outbound port requiring {@link RegistrationCI} interface.
	 */
	public ClientSubscriptionPlugin(String receivingInboundPortURI, ClientRegistrationOutboundPort registrationOutboundPort){
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()) : new PreconditionException("receivingInboundPortURI cannot be null or empty");
		assert(registrationOutboundPort != null) : new PreconditionException("registrationOutboundPort cannot be null");
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationOutboundPort = registrationOutboundPort;
	}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addOfferedInterface(ReceivingCI.class);

	}
	
	@Override
	public void	initialise() throws Exception {
		this.receivingInboundPort = new ClientReceivingPluginInboundPort(receivingInboundPortURI, this.getOwner(), this.getPluginURI());
		this.receivingInboundPort.publishPort();
		super.initialise();
	}
	
	@Override
	public void uninstall() throws Exception {
		this.receivingInboundPort.unpublishPort();
		this.removeOfferedInterface(ReceivingCI.class);
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------

	// Precondition and postcondition verifications are delegated to the port used
	// except for reception methods (on receivingInboundPort)
	
	@Override
	public boolean channelExist(String channel) {
		try {
			return registrationOutboundPort.channelExist(channel);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean channelAuthorised(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return registrationOutboundPort.channelAuthorised(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean subscribed(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return registrationOutboundPort.subscribed(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void subscribe(String channel, MessageFilterI filter)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			registrationOutboundPort.subscribe(receivingInboundPortURI, channel, filter);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void unsubscribe(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		try {
			registrationOutboundPort.unsubscribe(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException | NotSubscribedChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void modifyFilter(String channel, MessageFilterI filter) throws UnknownClientException,
			UnknownChannelException, UnauthorisedClientException, NotSubscribedChannelException {
		try {
			registrationOutboundPort.modifyFilter(receivingInboundPortURI, channel, filter);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException | NotSubscribedChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Receive passively {@code message} from {@code channel}; the call is
	 * executed asynchronously to free the client component thread as soon as
	 * the corresponding task is submitted to the receiver.
	 * 
	 * <p>
	 * Needs to be @Override for implementing a more personalized behavior on reception.
	 * If done so, the new version must ensure incrementing {@code nbMessagesReceived}.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel	name of an existing channel.
	 * @param message	message to be received from {@code channel}.
	 */
	@Override
	public void receive(String channel, MessageI message) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		nbMessagesReceived.incrementAndGet();
		
		Client client = (Client)this.getOwner();
		client.logMessage("Client received message '" + message + "' from the channel : " + channel);
	}
	
	/**
	 * Receive passively {@code messages} from {@code channel}; the call is
	 * executed asynchronously to free the client component thread as soon as
	 * the corresponding task is submitted to the receiver.
	 * 
	 * <p>
	 * Needs to be @Override for implementing a more personalized behavior on reception.
	 * If done so, the new version must ensure incrementing {@code nbMessagesReceived}.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.size() > 0}
	 * pre	{@code Stream.of(messages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel		name of the channel from which {@code messsage} is received.
	 * @param messages		array of messages received by the component.
	 */
	@Override
	public void receive(String channel, MessageI[] messages) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty());
		assert(messages != null && messages.length > 0);
		assert(Stream.of(messages).allMatch(m -> m != null));
		nbMessagesReceived.incrementAndGet();
		
		Client client = (Client)this.getOwner();
		client.logMessage("Client received multiple messages from the channel : " + channel);
		for (int i=0; i<messages.length; i++) {
			MessageI message = messages[i];
			client.logMessage("Message " + i + "  '" + message + "'");
		}
	}

	@Override
	public MessageI waitForNextMessage(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		// TODO
		return null;
	}

	@Override
	public MessageI waitForNextMessage(String channel, Duration d) throws UnknownClientException,
			UnknownChannelException, UnauthorisedClientException, NotSubscribedChannelException {
		// TODO
		return null;
	}

	@Override
	public Future<MessageI> getNextMessage(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		// TODO
		return null;
	}
	
	// -------------------------------------------------------------------------
	// Number of messages received getter
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the number of messages received since the start of the plugin.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code return >= 0}
	 * </pre>
	 * 
	 * @return the number of messages received since the start of the plugin.
	 */
	public int getNbMessagesReceived() {
		return nbMessagesReceived.get();
	}

}
