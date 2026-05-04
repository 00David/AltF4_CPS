package fr.sorbonne_u.broker.components.infos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * Class storing all information related to an existing channel on the {@link Broker}.
 * 
 * <p>
 * In case of multiple threads accessing on the same instance informations that can be modified, a global lock is used : 
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
 * 	<li> - Whether the channel is accessible only to privileged clients or to everyone (default channel).</li>
 * 	<li> - The client port that created the channel, or {@code null} if it was created by the {@link Broker}.</li>
 * 	<li> - The regular expression defining the accepted client port URIs for this channel.</li>
 * 	<li> - A FIFO queue of {@link MessageI} pending on the channel (to be sent).</li>
 * 	<li> - A counter indicating the number of threads currently unloading the channel, with its lock (both public).</li>
 * </ul>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
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

    /** The name of the channel */
    private final String channel;
    
    /** A mapping : subscriber client port URI -> its channel filter.*/
    private final Map<String, MessageFilterI> subscribers;
    
    /** Whether the channel is accessible only to privileged clients or to everyone */
    private String authorizedUsers;
    
    /** 
     * The client port that created the channel, or {@code null} if it was created by the {@link Broker}.<br>
     * If the creator unregistered itself from the system, the string remains unchanged here.
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
    public ChannelBrokerInfos(String channel,
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
    public synchronized void addOrReplaceSubscriber(String clientPortURI, MessageFilterI filter) {
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
    public synchronized boolean isSubscribed(String clientPortURI) {
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
    public synchronized void removeSubscriber(String clientPortURI) {
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
    public synchronized List<String> getSubscribers() {
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
    public synchronized void modifyAuthorizedUsers(String authorizedUsers) {
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
    public synchronized boolean isAuthorized(String clientPortURI) {
    	// Precondition check
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
    	return (clientPortURI.matches(authorizedUsers));
    }

    // -------------------------------------------------------------------------
    // CREATOR MANAGEMENT
    // -------------------------------------------------------------------------

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
    public boolean isCreator(String clientPortURI) {
    	// Precondition check
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
        return clientPortURI.equals(clientCreator); // clientCreator cannot be modified, so no need to protect it from a data race
    }
    
    /**
     * Returns the URI of the client port that created the channel. Can be {@code null} if the {@link Broker} has created the channel.
     * 
     * <p>
     * This method is used only in case of needing the creator string for a treatment not related to the channel, 
     * otherwise if it is only for checking if a user is the channel creator, use {@link ChannelBrokerInfos#isCreator(String)} method.
     * </p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} 	// no precondition
     * post  {@code true)} 	// no postcondition
     * </pre>
     * 
     * @return the URI of the client port that created the channel. Can be {@code null} if the {@link Broker} has created the channel.
     */
    public String getCreator() {
        return clientCreator; // clientCreator cannot be modified, so no need to protect it from a data race
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
    
    public synchronized int size() {
        return messages.size();
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