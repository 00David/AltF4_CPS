package fr.sorbonne_u.broker.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import fr.sorbonne_u.broker.component.tasks.LoaderTask;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Class storing all information related to an existing channel on the {@link Broker}.
 * 
 * <p>
 * In case of multiple threads accessing to informations that can be modified, a global lock is used : 
 * because of having only short operations, it does not cost a lot in terms of performances.
 * </p>
 * 
 * <p>
 * Each instance is uniquely identified by its {@code channel} attribute.
 * </p>
 * 
 * <p>
 * Maintained information includes :
 * </p>
 * <ul>
 * 	<li> - The channel name.</li>
 * 	<li> - A mapping : subscriber client port URI -> its channel filter.</li>
 * 	<li> - The regular expression defining the accepted client port URIs for this channel.</li>
 * 	<li> - The client port that created the channel, or {@code null} if it was created by the {@link Broker}.</li>
 * 	<li> - A FIFO queue of {@link MessageI} pending on the channel (to be sent).</li>
 * 	<li> - A counter indicating the number of threads currently unloading the channel, with its lock (both public).</li>
 * </ul>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code NB_MAX_BATCH_CHANNEL >= 1}
 * invariant	{@code NB_MAX_BATCH_STD_CHANNEL >= NB_MAX_BATCH_CHANNEL}
 * invariant	{@code NB_MAX_BATCH_PREM_CHANNEL >= NB_MAX_BATCH_STD_CHANNEL}
 * invariant	{@code channel != null && !channel.isEmpty()}
 * invariant	{@code subscribers != null}
 * invariant	{@code authorizedUsers != null}
 * invariant	{@code messages != null}
 * invariant	{@code nbActiveUnloaders != null}
 * invariant	{@code nbActiveUnloadersLock != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ChannelBrokerInfos {
	
    // -------------------------------------------------------------------------
    // STATIC CONSTANTS
    // -------------------------------------------------------------------------
	
	/** 
	 * The maximum number of messages in a batch sent from the channel
	 * when the creator is the {@code broker}, or has unregistered from the system. 
	 */
	public static final int NB_MAX_BATCH_CHANNEL = 1;
	
	/** 
	 * The maximum number of messages in a batch sent from the channel
	 * when the creator is creator is STANDARD. 
	 */
	public static final int NB_MAX_BATCH_STD_CHANNEL = 5;
	
	/** 
	 * The maximum number of messages in a batch sent from the channel
	 * when the creator is creator is PREMIUM. 
	 */
	public static final int NB_MAX_BATCH_PREM_CHANNEL = 10;
	
    // -------------------------------------------------------------------------
    // INTERNAL DATAS
    // -------------------------------------------------------------------------

    /** The name of the channel */
    private final String channel;
    
    /** A mapping : subscriber client port URI -> its channel filter.*/
    private final Map<String, MessageFilterI> subscribers;
    
    /** The regular expression defining the accepted client port URIs for this channel. */
    private String authorizedUsers;
    
    /** 
     * The client port that created the channel, or {@code null} if it was created by the {@link Broker}.<br>
     * If the creator unregistered itself from the system, the string remains unchanged here.
     * 
     * <p>The creator is final so no need to protect it from a data race.</p>
     */
    private final String clientCreator;
    
    /** A FIFO queue of messages pending on the channel (to be sent). */
    private final Queue<MessageI> messages;
    
    /** 
     * A counter indicating the number of threads currently unloading the channel, 
     * must be manipulated in a critical section synchronized on {@code nbActiveUnloadersLock}. 
     */
    public int nbActiveUnloaders = 0;
    
    /** {@code nbActiveUnloaders} lock. */
    public final Object nbActiveUnloadersLock = new Object();
    
    // -------------------------------------------------------------------------
    // CONSTRUCTOR
    // -------------------------------------------------------------------------

    /**
     * Constructs the information associated with a channel registered on the {@link Broker}.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code channel != null && !channel.isEmpty()}
     * pre  {@code authorisedUsers != null}
     * post class invariants satisfied
     * </pre>
     * 
     * @param channel         the channel name.
     * @param authorizedUsers the regular expression defining accepted client port URIs.
     * @param clientCreator   the URI of the client port that created the {@code channel}, 
     *                        or {@code null} if created by the {@link Broker}.
     */
    ChannelBrokerInfos(String channel,
                              String authorizedUsers,
                              String clientCreator) {
    	
    	if (channel == null || channel.isEmpty()) 
    		throw new IllegalArgumentException("A channel name cannot be null or empty");
        if (authorizedUsers == null) 
        	throw new IllegalArgumentException("The regex for authorised users cannot be null");

        this.channel = channel;
        this.subscribers = new HashMap<>();
        this.authorizedUsers = authorizedUsers;
        this.clientCreator = clientCreator;
        this.messages = new LinkedList<>();
    }

    // -------------------------------------------------------------------------
    // SUBSCRIBERS MANAGEMENT
    // -------------------------------------------------------------------------

    /**
     * Adds or replaces a subscribed {@code clientPortURI} on the channel.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * pre  {@code filter != null}
     * post {@code isSubscribed(clientPortURI)}
     * post {@code this.subscribers.get(clientPortURI).equals(filter)}
     * </pre>
     * 
     * @param clientPortURI					the client needed to be subscibed to the channel.
     * @param filter						the {@code clientPortURI}'s filter fo the channel.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     */
    synchronized void addOrReplaceSubscriber(String clientPortURI, MessageFilterI filter) {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (filter == null) 
    		throw new IllegalArgumentException("A filter cannot be null");
    	
    	subscribers.put(clientPortURI, filter);
    	
        // Postcondition checks
        assert isSubscribed(clientPortURI); // subscriber present
        assert subscribers.get(clientPortURI).equals(filter); // with the given filter
    }

    /**
     * Checks whether a given {@code clientPortURI} is subscribed.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * post {@code true} // no postcondition
     * </pre>
     * 
     * @param clientPortURI					the client for which we need to know if he is subscribed to the channel.
     * @return 								true if the client is subscribed to the channel, otherwise false
     * @throws IllegalArgumentException		if {@code clientPortURI} is null or empty.
     */
    synchronized boolean isSubscribed(String clientPortURI) {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
    	return subscribers.containsKey(clientPortURI);
    }

    /**
     * Removes a subscribed {@code clientPortURI}.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * pre  {@code isSubscribed(clientPortURI)}
     * post {@code !isSubscribed(clientPortURI)}
     * </pre>
     * 
     * @param clientPortURI					the client to unsubscribe from the channel.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     * 
     */
    synchronized void removeSubscriber(String clientPortURI) {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (!isSubscribed(clientPortURI)) 
    		throw new IllegalArgumentException("Client not already subscribed");

    	subscribers.remove(clientPortURI);   

        // Postcondition check
        assert !isSubscribed(clientPortURI);
    }

    /**
     * Returns the list of subscribed client port URIs.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} 	// no precondition
     * post  {@code return != null)}
     * </pre>
     * 
     * @return the list of client port URIs subscribed to the channel
     */
    synchronized List<String> getSubscribers() {
        List<String> subs = new ArrayList<>();
        
        for (Map.Entry<String, MessageFilterI> e : subscribers.entrySet()) {
        	String subscriber = e.getKey();
        	subs.add(subscriber);
        }
        
        return subs;
        // Postcondition verified by construction
    }

    // -------------------------------------------------------------------------
    // AUTHORIZED USERS MANAGEMENT
    // -------------------------------------------------------------------------

    /**
     * Updates the regular expression defining authorized users.
     * 
     * <p>Subscribed users that no longer match are automatically unsubscribed.</p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code authorizedUsers != null}
     * post {@code this.authorizedUsers == authorizedUsers)}
     * post {@code subscribers.keySet().stream().allMatch(subscriber -> subscriber.matches(authorizedUsers))}
     * </pre>
     * 
     * @param authorizedUsers 			the new regular expression defining authorized users.
     * @throws IllegalArgumentException if {@code authorizedUsers} is null.
     */
    synchronized void modifyAuthorizedUsers(String authorizedUsers) {
    	// Precondition check
    	if (authorizedUsers == null) 
    		throw new IllegalArgumentException("Authorized users cannot be null");
    	
    	this.authorizedUsers = authorizedUsers;
    	
        for (Map.Entry<String, MessageFilterI> e : subscribers.entrySet()) {
        	String subscriber = e.getKey();
        	if (!subscriber.matches(authorizedUsers)) {
    			removeSubscriber(subscriber);
    		}
        }
		        
    	// Postcondition check
    	assert subscribers.keySet().stream().allMatch(subscriber -> subscriber.matches(authorizedUsers));
    }

    /**
     * Indicates if the given {@code clientPortURI} is authorized to interact with the channel (its URI matches authorized users regex).
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre   {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * post  {@code return != null)}
     * </pre>
     * 
     * @param clientPortURI		the client for whom we verify its channel usage authorization.
     * @return 					True if {@code clientPortURI} is authorized to interact with the channel, otherwise False.
     */
    synchronized boolean isAuthorized(String clientPortURI) {
    	// Precondition check
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
    	return (clientPortURI.matches(authorizedUsers));
    }

    // -------------------------------------------------------------------------
    // CREATOR MANAGEMENT
    // -------------------------------------------------------------------------
    
    /**
     * Gets the maximum number of messages per batch sent from the channel, used by {@link LoaderTask}.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre   {@code clientsInfos != null}
     * post  {@code return >= 1)}
     * </pre>
     * 
     * @param clientsInfos					a mapping client inbound port (URI) -> its informations.
     * @throws IllegalArgumentException 	if a precondition fails.
     * @throws IllegalArgumentException 	the creator registration class is unknown.
     * @return 								the maximum number of messages per batch sent from the channel.
     */
    public int getNbMaxBatch(Map<String, ClientBrokerInfos> clientsInfos) {
    	// Precondition check
    	if (clientsInfos == null) {
    		throw new IllegalArgumentException("clientsInfos cannot be null");
    	}
    	
    	// If the creator is the broker, or he has been unregistered
    	if (clientCreator == null || !clientsInfos.containsKey(clientCreator)) {
    		return NB_MAX_BATCH_CHANNEL;
    	}
    	
    	RegistrationClass rc = clientsInfos.get(clientCreator).getRegistrationClass();
    	switch (rc) {
    		case FREE:
    			return NB_MAX_BATCH_CHANNEL;
    		case STANDARD:
    			return NB_MAX_BATCH_PREM_CHANNEL;
    		case PREMIUM:
    			return NB_MAX_BATCH_STD_CHANNEL;
    		default:
    			throw new IllegalArgumentException("Unknow registration class.");
    	}
    }

    /**
     * Indicates if the given {@code clientPortURI} has created the channel.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre   {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * post  {@code true)} 	// no postcondition
     * </pre>
     * 
     * @param clientPortURI		the client for whom we want to know if he is the channel creator.
     * @return 					True if {@code clientPortURI.equals(clientCreator)}, otherwise False.
     */
    boolean isCreator(String clientPortURI) {
    	// Precondition check
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
        return clientPortURI.equals(clientCreator);
    }
    
    /**
     * Returns the URI of the client port that created the channel. Can be {@code null} if the {@link Broker} has created the channel.
     * 
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} 	// no precondition
     * post  {@code true)} 	// no postcondition
     * </pre>
     * 
     * @return the URI of the client port that created the channel. Can be {@code null} if the {@link Broker} has created the channel.
     */
    String getCreator() {
        return clientCreator;
    }

    // -------------------------------------------------------------------------
    // MESSAGES MANAGEMENT
    // -------------------------------------------------------------------------

    /**
     * Loads a {@link MessageI} into the channel.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code msg != null}
     * post {@code messages.contains(msg)}
     * </pre>
     * 
     * @param msg						a message to load into the channel.
     * @throws IllegalArgumentException if the {@code msg} is null.
     */
    public synchronized void loadMessage(MessageI msg) {
    	// Precondition check
    	if (msg == null) throw new IllegalArgumentException("A message cannot be null");
        
        messages.add(msg);
        
        // Postcondition check
        assert messages.contains(msg);
    }

    /**
     * Unloads the next {@link MessageI} from the channel.
     * 
     * <p>It returns {@code null} if no message is available on the channel.</p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true}	// no precondition
     * post  {@code true}	// no postcondition
     * </pre>
     * 
     * @return the next {@link MessageI} from the channel, or {@code null} if no message is available into the channel.
     */
    public synchronized MessageI unloadMessage() {
        return messages.poll();
    }

    /**
     * Returns the list of subscriber URIs that accept the given message.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code msg != null}
     * post  {@code return != null)}
     * </pre>
     * 
     * @param msg						a message for which we will get users subscribed to the channel accepting it.
     * @return							the list of subscriber URIs that accept the given message.
     * @throws IllegalArgumentException	if the {@code msg} is null.
     */
    public synchronized List<String> targetSubscribers(MessageI msg) {
    	// Precondition check
    	if (msg == null) throw new IllegalArgumentException("A message cannot be null");

    	List<String> targets = new ArrayList<>();
    	for (Map.Entry<String, MessageFilterI> e : subscribers.entrySet()) {
    		String subscriber = e.getKey();
    		MessageFilterI filter = e.getValue();
    		if (filter.match(msg)) {
    			targets.add(subscriber);
    		}
    	}
    	
        return targets;
        // Postcondition verified by construction
    }

    // -------------------------------------------------------------------------
    // EQUALITY (based solely on channel name)
    // -------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChannelBrokerInfos that = (ChannelBrokerInfos) o;
        return channel.equals(that.channel);
    }

    @Override
    public int hashCode() {
        return channel.hashCode();
    }
}