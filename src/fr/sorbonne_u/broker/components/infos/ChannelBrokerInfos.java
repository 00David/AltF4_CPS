package fr.sorbonne_u.broker.components.infos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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
 * 	<li> - The channel name.
 * 	<li> - A mapping : subscriber client port URI -> its channel filter.
 * 	<li> - Whether the channel is accessible only to privileged clients or to everyone (default channel).
 * 	<li> - The client port that created the channel, or {@code null} if it was created by the {@link Broker}.
 * 	<li> - The regular expression defining the accepted client port URIs for this channel.
 * 	<li> - A FIFO queue of {@link MessageI} pending on the channel (to be sent).
 * </ul>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code channel != null && !channel.isEmpty()}
 * invariant	{@code subscribers != null}
 * invariant	{@code authorizedUsers != null}
 * invariant	{@code messages != null}
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
    private final boolean onlyPrivileged;
    
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
     * @param onlyPrivileged  true if accessible only to privileged clients, false otherwise.
     */
    public ChannelBrokerInfos(String channel,
                              String authorizedUsers,
                              String clientCreator,
                              boolean onlyPrivileged) {
    	
    	if (channel == null || channel.isEmpty()) 
    		throw new IllegalArgumentException("A channel name cannot be null or empty");
        if (authorizedUsers == null) throw new IllegalArgumentException("The regex for authorised users cannot be null");

        this.channel = channel;
        this.subscribers = new ConcurrentHashMap<>();
        this.onlyPrivileged = onlyPrivileged;
        this.authorizedUsers = authorizedUsers;
        this.clientCreator = clientCreator;
        this.messages = new ConcurrentLinkedQueue<>();
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
     * </pre>
     * 
     * @param clientPortURI					the client needed to be subscibed to the channel.
     * @param filter						the {@code clientPortURI}'s filter fo the channel.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     */
    public synchronized void addOrReplaceSubscriber(String clientPortURI, MessageFilterI filter) {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (filter == null) throw new IllegalArgumentException("A filter cannot be null");
    	
    	subscribers.put(clientPortURI, filter);
    	
        // Postcondition check
        assert isSubscribed(clientPortURI);
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
    	if (clientPortURI == null || clientPortURI.isEmpty()) throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
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
    	if (clientPortURI == null || clientPortURI.isEmpty()) throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (!isSubscribed(clientPortURI)) throw new IllegalArgumentException("Client not already subscribed");

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
     * Indicates whether the channel is restricted to privileged clients.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} 	// no precondition
     * post  {@code true)} 	// no postcondition
     * </pre>
     * 
     * @return true if this channel is only for privileged clients, otherwise false
     */
    public boolean onlyPrivileged() {
        return onlyPrivileged;  // onlyPrivileged cannot be modified, so no need to protect it from a data race
    }

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
    	if (authorizedUsers == null) throw new IllegalArgumentException("Authorized users cannot be null");
    	
    	this.authorizedUsers = authorizedUsers;
		
    	for (String subscriber : getSubscribers()) {
    		if (!subscriber.matches(authorizedUsers)) {
    			removeSubscriber(subscriber);
    		}
    	}
		        
    	// Postcondition check
    	assert subscribers.keySet().stream().allMatch(subscriber -> subscriber.matches(authorizedUsers));
    }

    /**
     * Returns the regular expression filtering authorized client URIs.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} 	// no precondition
     * post  {@code return != null)}
     * </pre>
     * 
     * @return the regular expression filtering authorized client URIs.
     */
    public synchronized String getAuthorizedUsers() {
    	return authorizedUsers;
    }

    // -------------------------------------------------------------------------
    // CREATOR MANAGEMENT
    // -------------------------------------------------------------------------

    /**
     * Returns the URI of the client port that created the channel. Can be {@code null} if the {@link Broker} has created the channel.
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
     * Loads multiple {@link MessageI} instances into the channel.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code msgs != null && !msgs.isEmpty()}
     * post {@code messages.containsAll(msgs)}
     * </pre>
     * 
     * @param msgs						a list of messages to load into the channel.
     * @throws IllegalArgumentException if {@code msgs} is null or empty.
     */
    public synchronized void loadMessages(List<MessageI> msgs) {
    	// Precondition checks
    	if (msgs == null || msgs.isEmpty()) throw new IllegalArgumentException("Messages cannot be null or empty");
        
        messages.addAll(msgs);
        
        // Postcondition check
        assert messages.containsAll(msgs);
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