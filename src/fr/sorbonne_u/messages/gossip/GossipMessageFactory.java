package fr.sorbonne_u.messages.gossip;

import java.util.ArrayList;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.gossip.GossipMessage.Action;
import fr.sorbonne_u.messages.gossip.GossipMessage.GossipInternalMessage;

/**
 * A factory class, being the only way of creating personalized {@link GossipMessage}s (outside the class itself).
 *
 * <p>
 * This way of creating those messages prevents doing an important number of small classes, that would share only little differences, 
 * and that would not necessarily be the cleanest way of manipulating gossip messages, from creation to content getting.
 * </p>
 * 
 * <p>
 * A gossip message is always created with the carried method call parameters being stored as properties
 * of an internal {@link GossipInternalMessage}.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true} // no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class GossipMessageFactory {

	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#publishService(String, String, ArrayList, boolean)} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messagesToPublish != null && !messagesToPublish.isEmpty()}
	 * post	{@code return != null}
	 * post	{@code return.messageAction == Action.PUBLISH}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI	           	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           	Name of the channel on which {@code message} must be published.
	 * @param messagesToPublish			    List of messages to be published on {@code channel}. It can contain an only message.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#publishService(String, String, ArrayList, boolean)
	 */
	public static GossipMessage createPublishMessages(String emitterURI, String receptionPortURI, 
			String channel, ArrayList<MessageI> messagesToPublish) {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) 
			throw new IllegalArgumentException("channel cannot be null or empty");
		if (messagesToPublish == null || messagesToPublish.isEmpty()) 
			throw new IllegalArgumentException("messagesToPublish cannot be null or empty");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_PUBLISH.get(0), receptionPortURI);
		carriedMessage.putProperty(GossipMessage.ARGS_PUBLISH.get(1), channel);
		carriedMessage.putProperty(GossipMessage.ARGS_PUBLISH.get(2), messagesToPublish);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.PUBLISH;
		return gMsg;
	}
	
	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#registerService(String, RegistrationClass, boolean)} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.REGISTER_CLIENT}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI	          	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				          	The required registration class.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#registerService(String, RegistrationClass, boolean)
	 */
	public static GossipMessage createRegisterMessage(String emitterURI, String receptionPortURI, RegistrationClass rc) {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (rc == null) 
			throw new IllegalArgumentException("rc cannot be null");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_REGISTER_CLIENT.get(0), receptionPortURI);
		carriedMessage.putProperty(GossipMessage.ARGS_REGISTER_CLIENT.get(1), rc);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.REGISTER_CLIENT;
		return gMsg;
	}
	
	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#unregisterService(String, boolean)} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.UNREGISTER_CLIENT}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#unregisterService(String, boolean)
	 */
	public static GossipMessage createUnregisterMessage(String emitterURI, String receptionPortURI) {
		// Precondition check
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_UNREGISTER_CLIENT.get(0), receptionPortURI);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.UNREGISTER_CLIENT;
		return gMsg;
	}
	
	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#createChannelService(String, String, String, boolean)} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code autorisedUsers == null || !autorisedUsers.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.CREATE_CHANNEL}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI				Identifier of the requesting component <i>i.e.</i>, URI of its inbound port 
	 * 										offering the component interface {@code ReceivingCI}.
	 * @param channel						Name of the channel to be created.
	 * @param autorizedUsers				Regular expression that matches the URIs of components that must be authorized to use the new channel.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#createChannelService(String, String, String, boolean)
	 */
	public static GossipMessage createCreateChannelMessage(String emitterURI, String receptionPortURI, String channel, String autorizedUsers) {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) 
			throw new IllegalArgumentException("channel cannot be null or empty");
		if (autorizedUsers == null || autorizedUsers.isEmpty()) 
			throw new IllegalArgumentException("autorizedUsers cannot be null or empty");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_CREATE_CHANNEL.get(0), receptionPortURI);
		carriedMessage.putProperty(GossipMessage.ARGS_CREATE_CHANNEL.get(1), channel);
		carriedMessage.putProperty(GossipMessage.ARGS_CREATE_CHANNEL.get(2), autorizedUsers);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.CREATE_CHANNEL;
		return gMsg;
	}
	
	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#destroyChannelService(String, String, boolean)} 
	 * or a {@link Broker#destroyChannelNowService(String, String, boolean)} call, spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.DESTROY_CHANNEL}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						Name of the channel to be destroyed.
	 * @param now							True if {@code destroyChannelNowService} is called. False if {@code destroyChannelService} is called.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#destroyChannelService(String, String, boolean)
	 * @see Broker#destroyChannelNowService(String, String, boolean)
	 */
	public static GossipMessage createDestroyChannelMessage(String emitterURI, String receptionPortURI, String channel, boolean now) {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) 
			throw new IllegalArgumentException("channel cannot be null or empty");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_DESTROY_CHANNEL.get(0), receptionPortURI);
		carriedMessage.putProperty(GossipMessage.ARGS_DESTROY_CHANNEL.get(1), channel);
		carriedMessage.putProperty(GossipMessage.ARGS_DESTROY_CHANNEL.get(2), now);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.DESTROY_CHANNEL;
		return gMsg;
	}
	
	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#subscribeService(String, String, MessageFilterI, boolean)} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.SUBSCRIBE_CLIENT_CHANNEL}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI       		URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			      		Name of an existing channel.
	 * @param filter			           	Filter that accepts the message the component wants to receive.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#subscribeService(String, String, MessageFilterI, boolean)
	 */
	public static GossipMessage createSubscribeClientMessage(String emitterURI, String receptionPortURI, String channel, MessageFilterI filter) {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) 
			throw new IllegalArgumentException("channel cannot be null or empty");
		if (filter == null) 
			throw new IllegalArgumentException("filter cannot be null");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(0), receptionPortURI);
		carriedMessage.putProperty(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(1), channel);
		carriedMessage.putProperty(GossipMessage.ARGS_SUBSCRIBE_CLIENT_CHANNEL.get(2), filter);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.SUBSCRIBE_CLIENT_CHANNEL;
		return gMsg;
	}
	
	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#unsubscribeService(String, String, boolean)} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.UNSUBSCRIBE_CLIENT_CHANNEL}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			            Name of an existing channel.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#unsubscribeService(String, String, boolean)
	 */
	public static GossipMessage createUnsubscribeClientMessage(String emitterURI, String receptionPortURI, String channel) {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) 
			throw new IllegalArgumentException("channel cannot be null or empty");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_UNSUBSCRIBE_CLIENT_CHANNEL.get(0), receptionPortURI);
		carriedMessage.putProperty(GossipMessage.ARGS_UNSUBSCRIBE_CLIENT_CHANNEL.get(1), channel);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.UNSUBSCRIBE_CLIENT_CHANNEL;
		return gMsg;
	}
	
	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#modifyServiceClassService(String, RegistrationClass, boolean)} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.UPDATE_CLIENT_SERVICE_CLASS}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI	            URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				            The required registration class.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#modifyServiceClassService(String, RegistrationClass, boolean)
	 */
	public static GossipMessage createUpdateClientClassMessage(String emitterURI, String receptionPortURI, RegistrationClass rc)  {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (rc == null) 
			throw new IllegalArgumentException("rc cannot be null");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_UPDATE_CLIENT_SERVICE_CLASS.get(0), receptionPortURI);
		carriedMessage.putProperty(GossipMessage.ARGS_UPDATE_CLIENT_SERVICE_CLASS.get(1), rc);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.UPDATE_CLIENT_SERVICE_CLASS;
		return gMsg;
	}
	
	/**
	 * Creates a gossip message carrying the parameters for a {@link Broker#modifyAuthorisedUsersService(String, String, String, boolean)} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code autorisedUsers != null && !autorisedUsers.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.UPDATE_CHANNEL_AUTHORIZED_URIS}
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param receptionPortURI				Identifier of the requesting component <i>i.e.</i>, URI of its inbound port 
	 * 										offering the component interface {@code ReceivingCI}.
	 * @param channel						Name of the channel.
	 * @param autorizedUsers				Regular expression that matches the URIs of components that must be authorized to use the new channel.
	 * @return								The corresponding gossip message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#modifyAuthorisedUsersService(String, String, String, boolean)
	 */
	public static GossipMessage createUpdateChannelAuthURIsMessage(String emitterURI, String receptionPortURI, String channel, String autorizedUsers)  {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (receptionPortURI == null || receptionPortURI.isEmpty()) 
			throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) 
			throw new IllegalArgumentException("channel cannot be null or empty");
		if (autorizedUsers == null || autorizedUsers.isEmpty()) 
			throw new IllegalArgumentException("autorizedUsers cannot be null or empty");
		
		GossipMessage gMsg = new GossipMessage(emitterURI);
		GossipInternalMessage carriedMessage = new GossipInternalMessage();
		carriedMessage.putProperty(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(0), receptionPortURI);
		carriedMessage.putProperty(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(1), channel);
		carriedMessage.putProperty(GossipMessage.ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS.get(2), autorizedUsers);
		gMsg.message = carriedMessage;
		gMsg.messageAction = Action.UPDATE_CHANNEL_AUTHORIZED_URIS;
		return gMsg;
	}
}
