package fr.sorbonne_u.messages;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Random;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * The Message class implementing a gossip message for the publication/subscription system.<br>
 * It does not necessarily carry publication messages.
 * 
 * <p>A GossipMessage is identified solely by its {@code messageURI}.</p>
 * 
 * <p>Personalized methods in this class must be used to set the carried message. Thus they also define the protocol usage.</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code emitterURI != null && !emitterURI.isEmpty()}
 * invariant	{@code messageURI != null && !messageURI.isEmpty()}
 * invariant	{@code timestamp != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class GossipMessage implements GossipMessageI{

	private static final long serialVersionUID = 3248301193711261691L;
	
	/**
	 * Each GossipMessage carrying a {@link MessageI} has an attached action, giving the purpose of the message.<br>
	 * If the message content has not yet been set, then the default action value is {@code NOTHING}.
	 */
	public static enum Action {NOTHING, PUBLISH, REGISTER_CLIENT, UNREGISTER_CLIENT, CREATE_CHANNEL, DESTROY_CHANNEL, 
		SUBSCRIBE_CLIENT_CHANNEL, UNSUBSCRIBE_CLIENT_CHANNEL, UPDATE_CLIENT_SERVICE_CLASS, UPDATE_CHANNEL_AUTHORIZED_URIS};
	
	/** The URI of the component emiting the gossip message. */
	public final String emitterURI;
	
	/** The URI of the gossip message, created by the {@code generateGossipMessageURI} method. */
	private final String messageURI;
	
	/** Gives the action needed for the carried {@code message}. */
	private Action messageAction;
	
	/** A carried message. In its properties it has all needed parameters for the linked Broker {@code action}. */
	private MessageI message;
	
	/** The timestamp of when the gossip message is created (set in its constructor). */
	private final Instant timestamp;
	
	//--------------------------------------------------------------------------
	// Constructors
	//--------------------------------------------------------------------------
	
	/**
	 * Creates an empty gossip message, from the original {@code emitterURI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @throws IllegalArgumentException		If {@code emitterURI} is null or empty.
	 */
	public GossipMessage(String emitterURI) {
		// Precondition check
		if (emitterURI == null || emitterURI.isEmpty()) throw new IllegalArgumentException("emitterURI cannot be null or empty");
		
		this.emitterURI = emitterURI;
		this.messageURI = generateGossipMessageURI();
		this.messageAction = Action.NOTHING;
		this.timestamp = Instant.now();
	}
	
	/**
	 * Private constructor for creating an empty gossip message, from {@code emitterURI}, with a given existing {@code messageURI}.<br>
	 * Used in the {@code copyWithNewEmitterURI} method for keeping the {@code messageURI} in the copy.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code emitterURI != null && !emitterURI.isEmpty()}
	 * pre	{@code messageURI != null && !messageURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param emitterURI 					The URI of the component creating this gossip message. 
	 * @param messageURI 					The URI of an existing GossipMessage.
	 * @throws IllegalArgumentException		If {@code emitterURI} is null or empty.
	 * @throws IllegalArgumentException		If {@code messageURI} is null or empty.
	 */
	private GossipMessage(String emitterURI, String messageURI) {
		// Precondition checks
		if (emitterURI == null || emitterURI.isEmpty()) throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (messageURI == null || messageURI.isEmpty()) throw new IllegalArgumentException("emitterURI cannot be null or empty");
		
		this.emitterURI = emitterURI;
		this.messageURI = messageURI;
		this.messageAction = Action.NOTHING;
		this.timestamp = Instant.now();
	}
	
	/**
	 * Generates a URI for the gossip message. The generated URI has the form : 'emitterURI'_'generatedString'.<br>
	 * The generated string has 20 characters.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code return != null && !return.isEmpty()}
	 * </pre>
	 * 
	 * @return the gossip message generated URI.
	 */
	private String generateGossipMessageURI() {
		int generatedStringLength = 20;
		
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        
        StringBuilder result = new StringBuilder(generatedStringLength);
        for (int i = 0; i < generatedStringLength; i++) result.append(chars.charAt(random.nextInt(chars.length())));
        
        return emitterURI+"_"+result.toString();
	}
	
    // -------------------------------------------------------------------------
    // EQUALITY (based solely on messageURI)
    // -------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GossipMessage that = (GossipMessage) o;
        return messageURI.equals(that.messageURI);
    }

    @Override
    public int hashCode() {
        return messageURI.hashCode();
    }
	
	//--------------------------------------------------------------------------
	// Getters
	//--------------------------------------------------------------------------

	@Override
	public String gossipMessageURI() {
		return messageURI;
	}

	@Override
	public Instant timestamp() {
		return timestamp;
	}
	
	/**
	 * Returns the action attached to the carried message. It is NOTHING if no message has yet been set.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * @return the action attached to the carried message.
	 */
	public Action messageAction() {
		return messageAction;
	}
	
	/**
	 * Returns the carried {@link MessageI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the carried message, {@code null} if {@code messageAction == Action.NOTHING}.
	 */
	public MessageI message() {
		return message;
	}
	
	//--------------------------------------------------------------------------
	// Copy
	//--------------------------------------------------------------------------

	@Override
	public GossipMessageI copyWithNewEmitterURI(String newGossipEmitterURI) {
		// Precondition check
		if (newGossipEmitterURI == null || newGossipEmitterURI.isEmpty()) throw new IllegalArgumentException("newGossipEmitterURI cannot be null or empty");
		
		GossipMessage newMessage = new GossipMessage(newGossipEmitterURI, this.messageURI); // The same messageURI is kept in the copy
		newMessage.messageAction = this.messageAction;
		if (this.message != null) newMessage.message = this.message.copy();
		return newMessage;
	}
	
	//--------------------------------------------------------------------------
	// Setting message
	//--------------------------------------------------------------------------

	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code publishService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messagesToPublish != null && !messagesToPublish.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.PUBLISH}
	 * </pre>
	 * 
	 * @param receptionPortURI	           	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			           	Name of the channel on which {@code message} must be published.
	 * @param messagesToPublish			    List of messages to be published on {@code channel}. It can contain an only message.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#publishService(String, String, ArrayList, boolean)
	 */
	public void setPublishMessages(String receptionPortURI, String channel, ArrayList<MessageI> messagesToPublish) {
		// Precondition checks
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) throw new IllegalArgumentException("channel cannot be null or empty");
		if (messagesToPublish == null || messagesToPublish.isEmpty()) throw new IllegalArgumentException("messagesToPublish cannot be null or empty");
		
		Message carriedMessage = new Message();
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		carriedMessage.putProperty("channel", channel);
		carriedMessage.setPayload(messagesToPublish); // the message to publish is set as payload, ONLY THE CASE IN THIS METHOD.
		this.message = carriedMessage;
		this.messageAction = Action.PUBLISH;
	}
	
	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code registerService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.REGISTER_CLIENT}
	 * </pre>
	 * 
	 * @param receptionPortURI	          	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				          	The required registration class.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#registerService(String, RegistrationClass, boolean)
	 */
	public void setRegisterMessage(String receptionPortURI, RegistrationClass rc) {
		// Precondition checks
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (rc == null) throw new IllegalArgumentException("rc cannot be null");
		
		Message carriedMessage = new Message();
		// parameters for Broker 'registerService' method as properties
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		carriedMessage.putProperty("rc", rc);
		this.message = carriedMessage;
		this.messageAction = Action.REGISTER_CLIENT;
	}
	
	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code unregisterService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.UNREGISTER_CLIENT}
	 * </pre>
	 * 
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#unregisterService(String, boolean)
	 */
	public void setUnregisterMessage(String receptionPortURI) {
		// Precondition check
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		
		Message carriedMessage = new Message();
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		this.message = carriedMessage;
		this.messageAction = Action.UNREGISTER_CLIENT;
	}
	
	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code createChannelService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code autorisedUsers == null || !autorisedUsers.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.CREATE_CHANNEL}
	 * </pre>
	 * 
	 * @param receptionPortURI				Identifier of the requesting component <i>i.e.</i>, URI of its inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						Name of the channel to be created.
	 * @param autorizedUsers				Regular expression that matches the URIs of components that must be authorized to use the new channel.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#createChannelService(String, String, String, boolean)
	 */
	public void setCreateChannelMessage(String receptionPortURI, String channel, String autorizedUsers) {
		// Precondition checks
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) throw new IllegalArgumentException("channel cannot be null or empty");
		if (autorizedUsers == null || autorizedUsers.isEmpty()) throw new IllegalArgumentException("autorizedUsers cannot be null or empty");
		
		Message carriedMessage = new Message();
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		carriedMessage.putProperty("channel", channel);
		carriedMessage.putProperty("autorizedUsers", autorizedUsers);
		this.message = carriedMessage;
		this.messageAction = Action.CREATE_CHANNEL;
	}
	
	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code destroyChannelService} or a {@code destroyChannelNowService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.DESTROY_CHANNEL}
	 * </pre>
	 * 
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						Name of the channel to be destroyed.
	 * @param now							True if {@code destroyChannelNowService} is called. False if {@code destroyChannelService} is called.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#destroyChannelService(String, String, boolean)
	 * @see Broker#destroyChannelNowService(String, String, boolean)
	 */
	public void setDestroyChannelMessage(String receptionPortURI, String channel, boolean now) {
		// Precondition checks
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) throw new IllegalArgumentException("channel cannot be null or empty");
		
		Message carriedMessage = new Message();
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		carriedMessage.putProperty("channel", channel);
		carriedMessage.putProperty("now", now);
		this.message = carriedMessage;
		this.messageAction = Action.DESTROY_CHANNEL;
	}
	
	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code subscribeService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.SUBSCRIBE_CLIENT_CHANNEL}
	 * </pre>
	 * 
	 * @param receptionPortURI       		URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			      		Name of an existing channel.
	 * @param filter			           	Filter that accepts the message the component wants to receive.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#subscribeService(String, String, MessageFilterI, boolean)
	 */
	public void setSubscribeClientMessage(String receptionPortURI, String channel, MessageFilterI filter) {
		// Precondition checks
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) throw new IllegalArgumentException("channel cannot be null or empty");
		if (filter == null) throw new IllegalArgumentException("filter cannot be null");
		
		Message carriedMessage = new Message();
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		carriedMessage.putProperty("channel", channel);
		carriedMessage.putProperty("filter", filter);
		this.message = carriedMessage;
		this.messageAction = Action.SUBSCRIBE_CLIENT_CHANNEL;
	}
	
	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code unsubscribeService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.UNSUBSCRIBE_CLIENT_CHANNEL}
	 * </pre>
	 * 
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel			            Name of an existing channel.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#unsubscribeService(String, String, boolean)
	 */
	public void setUnsubscribeClientMessage(String receptionPortURI, String channel) {
		// Precondition checks
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) throw new IllegalArgumentException("channel cannot be null or empty");
		
		Message carriedMessage = new Message();
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		carriedMessage.putProperty("channel", channel);
		this.message = carriedMessage;
		this.messageAction = Action.UNSUBSCRIBE_CLIENT_CHANNEL;
	}
	
	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code modifyServiceClassService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.UPDATE_CLIENT_SERVICE_CLASS}
	 * </pre>
	 * 
	 * @param receptionPortURI	            URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				            The required registration class.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#modifyServiceClassService(String, RegistrationClass, boolean)
	 */
	public void setUpdateClientClassMessage(String receptionPortURI, RegistrationClass rc)  {
		// Precondition checks
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (rc == null) throw new IllegalArgumentException("rc cannot be null");
		
		Message carriedMessage = new Message();
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		carriedMessage.putProperty("rc", rc);
		this.message = carriedMessage;
		this.messageAction = Action.UPDATE_CLIENT_SERVICE_CLASS;
	}
	
	/**
	 * Sets the content of a gossip message carrying the parameters for a {@code modifyAuthorisedUsersService} call, 
	 * spread to other {@link Broker} components.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code autorisedUsers != null && !autorisedUsers.isEmpty()}
	 * post	{@code this.message != null}
	 * post	{@code this.messageAction == Action.UPDATE_CHANNEL_AUTHORIZED_URIS}
	 * </pre>
	 * 
	 * @param receptionPortURI				Identifier of the requesting component <i>i.e.</i>, URI of its inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						Name of the channel.
	 * @param autorizedUsers				Regular expression that matches the URIs of components that must be authorized to use the new channel.
	 * @throws IllegalArgumentException 	If a precondition fails.
	 * @see Broker#modifyAuthorisedUsersService(String, String, String, boolean)
	 */
	public void setUpdateChannelAuthURIsMessage(String receptionPortURI, String channel, String autorizedUsers)  {
		// Precondition checks
		if (receptionPortURI == null || receptionPortURI.isEmpty()) throw new IllegalArgumentException("receptionPortURI cannot be null or empty");
		if (channel == null || channel.isEmpty()) throw new IllegalArgumentException("channel cannot be null or empty");
		if (autorizedUsers == null || autorizedUsers.isEmpty()) throw new IllegalArgumentException("autorizedUsers cannot be null or empty");
		
		Message carriedMessage = new Message();
		carriedMessage.putProperty("receptionPortURI", receptionPortURI);
		carriedMessage.putProperty("channel", channel);
		carriedMessage.putProperty("autorizedUsers", autorizedUsers);
		this.message = carriedMessage;
		this.messageAction = Action.UPDATE_CHANNEL_AUTHORIZED_URIS;
	}
}
