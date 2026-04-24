package fr.sorbonne_u.messages.gossip;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.messages.Message;

/**
 * The message class implementing a gossip message for the publication/subscription system,
 * solely identified by its (supposed) unique {@code messageURI}.
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
	
	//--------------------------------------------------------------------------
	// Constants defining gossip message contents (see their use in GossipMessageFactory)
	//--------------------------------------------------------------------------
	
	/**
	 * Each GossipMessage carrying a {@link Message} has an attached action, giving the purpose of the message.<br>
	 * If the message content has not yet been set, then the default action value is {@code NOTHING}.
	 */
	public static enum Action {NOTHING, PUBLISH, REGISTER_CLIENT, UNREGISTER_CLIENT, CREATE_CHANNEL, DESTROY_CHANNEL, 
		SUBSCRIBE_CLIENT_CHANNEL, UNSUBSCRIBE_CLIENT_CHANNEL, UPDATE_CLIENT_SERVICE_CLASS, UPDATE_CHANNEL_AUTHORIZED_URIS};
		
	/** Unmodifiable list containing internal message property names for a publish message. */
	public static final List<String> ARGS_PUBLISH = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_PM", "channel_PM", "messages_PM"));
	
	/** Unmodifiable list containing internal message property names for a client registration message. */
	public static final List<String> ARGS_REGISTER_CLIENT = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_RC", "rc_RC"));
	
	/** Unmodifiable list containing internal message property names for a client unregistration message. */
	public static final List<String> ARGS_UNREGISTER_CLIENT = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_UC"));
	
	/** Unmodifiable list containing internal message property names for a channel creation message. */
	public static final List<String> ARGS_CREATE_CHANNEL = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_CC", "channel_CC", "autorizedUsers_CC"));
	
	/** Unmodifiable list containing internal message property names for a channel destruction message. */
	public static final List<String> ARGS_DESTROY_CHANNEL = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_DC", "channel_DC", "now_DC"));
	
	/** Unmodifiable list containing internal message property names for a client channel subscription message. */
	public static final List<String> ARGS_SUBSCRIBE_CLIENT_CHANNEL = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_SCC", "channel_SCC", "filter_SCC"));
	
	/** Unmodifiable list containing internal message property names for a client channel unsubscription message. */
	public static final List<String> ARGS_UNSUBSCRIBE_CLIENT_CHANNEL = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_UCC", "channel_UCC"));
	
	/** Unmodifiable list containing internal message property names for a client registration class modification message. */
	public static final List<String> ARGS_UPDATE_CLIENT_SERVICE_CLASS = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_UCSC", "rc_UCSC"));
	
	/** Unmodifiable list containing internal message property names for a channel authorized users modification message. */
	public static final List<String> ARGS_UPDATE_CHANNEL_AUTHORIZED_URIS = 
			Collections.unmodifiableList(Arrays.asList("receptionPortURI_UCAU", "channel_UCAU", "autorizedUsers_UCAU"));
	
	//--------------------------------------------------------------------------
	// Internal attributes
	//--------------------------------------------------------------------------
	
	/** The URI of the component emiting the gossip message. */
	public final String emitterURI;
	
	/** The URI of the gossip message, created by the {@code generateGossipMessageURI} method. */
	protected final String messageURI;
	
	/** Gives the action needed for the carried {@code message}. */
	protected Action messageAction;
	
	/** A carried message. In its properties it has all needed parameters for the linked Broker {@code action}. */
	protected MessageI message;
	
	/** The timestamp of when the gossip message is created (set in its constructor). */
	protected final Instant timestamp;
	
	//--------------------------------------------------------------------------
	// Constructors
	//--------------------------------------------------------------------------
	
	/**
	 * Creates an empty gossip message, from the original {@code emitterURI}.<br>
	 * Used only by {@link GossipMessageFactory}.
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
	protected GossipMessage(String emitterURI) {
		// Precondition check
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		
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
		if (emitterURI == null || emitterURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		if (messageURI == null || messageURI.isEmpty()) 
			throw new IllegalArgumentException("emitterURI cannot be null or empty");
		
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
		if (newGossipEmitterURI == null || newGossipEmitterURI.isEmpty()) 
			throw new IllegalArgumentException("newGossipEmitterURI cannot be null or empty");
		
		GossipMessage newMessage = new GossipMessage(newGossipEmitterURI, this.messageURI); // The same messageURI is kept in the copy
		newMessage.messageAction = this.messageAction;
		if (this.message != null) newMessage.message = this.message.copy();
		return newMessage;
	}
}
