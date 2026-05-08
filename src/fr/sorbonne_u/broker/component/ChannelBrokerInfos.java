package fr.sorbonne_u.broker.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import fr.sorbonne_u.broker.component.tasks.LoaderTask;
import fr.sorbonne_u.broker.component.tasks.UnloaderTask;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Class storing all information related to an existing channel on the {@link Broker} 
 * AND implementing operations on channels.
 * 
 * <p>
 * In case of multiple threads accessing to informations that can be modified, <strong>each internal information is protected
 * by its own lock (or synchronized monitor)</strong> (EXCEPT for {@code this.clientCreator} which is final) : 
 * channels can often be accessed simultaneously by multiple threads for manipulating different datas, and some of those 
 * manipulations are not simple lookups / adds, so it seems relevant in terms of concurrent granularity to have one lock per data.
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
 * 	<li> - A counter indicating the number of threads currently unloading the channel.</li>
 * </ul>
 * 
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code NB_MAX_BATCH_CHANNEL >= 1}
 * invariant	{@code NB_MAX_BATCH_STD_CHANNEL >= NB_MAX_BATCH_CHANNEL}
 * invariant	{@code NB_MAX_BATCH_PREM_CHANNEL >= NB_MAX_BATCH_STD_CHANNEL}
 * 
 * invariant	{@code channel != null && !channel.isEmpty()}
 * invariant	{@code subscribers != null}
 * invariant 	{@code subscribers.values().stream().allMatch(f -> f != null)}
 * invariant 	{@code subscribers.keySet().stream().allMatch(s -> s != null)}
 * invariant	{@code authorizedUsers != null}
 * invariant	{@code messages != null}
 * invariant 	{@code messages.stream().allMatch(m -> m != null)}
 * invariant	{@code nbActiveUnloaders >= 0}
 * 
 * invariant	FORCED LOCK ORDER : authorizedUsersLock -> subscribersLock
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
	
    /**
     * Maximum ratio of active unloader threads allowed per channel.
     * 
     * <p>
     * For example, with a ratio of 2 and 10 total unloader threads:
     * a channel can have at most 5 concurrent unloaders (10 / 2 = 5).
     * </p>
     */
    public static final int MAX_ACTIVE_UNLOADERS_RATIO = 2;
	
    // -------------------------------------------------------------------------
    // INTERNAL DATAS
    // -------------------------------------------------------------------------

    /** The name of the channel. */
    private final String channel;
    
    /** 
     * A mapping : subscriber client port URI -> its channel filter.
     * <p>Multi-thread accesses are handled by {@link ChannelBrokerInfos#subscribersLock}.</p>
     */
    private final Map<String, MessageFilterI> subscribers;
    private final ReadWriteLock subscribersLock = new ReentrantReadWriteLock(true); // fair
    
    /** 
     * The regular expression defining the accepted client port URIs for this channel.
     * <p>Multi-thread accesses are handled by {@link ChannelBrokerInfos#authorizedUsersLock}.</p>
     */
    private String authorizedUsers;
    private final ReadWriteLock authorizedUsersLock = new ReentrantReadWriteLock(true); // fair
    
    /** 
     * The client port that created the channel, or {@code null} if it was created by the {@link Broker}.<br>
     * If the creator unregistered itself from the system, the string remains unchanged here.
     * <p>The creator is final so no need to protect it from a data race.</p>
     */
    private final String clientCreator;
    
    /** 
     * A FIFO queue of messages pending on the channel (to be sent).
     * <p>Multi-thread accesses are handled by its synchronized lock. No Read/Write lock because it is only used in Write.</p>
     */
    private final Queue<MessageI> messages;
    
    /** 
     * A counter indicating the number of threads currently unloading the channel.
     * <p>Multi-thread accesses are handled by {@link ChannelBrokerInfos#nbActiveUnloadersLock}.</p>
     */
    private int nbActiveUnloaders = 0;
    private final Lock nbActiveUnloadersLock = new ReentrantLock(true); // fair
    
    // -------------------------------------------------------------------------
    // CONSTRUCTOR
    // -------------------------------------------------------------------------

    /**
     * Constructs the information associated with a channel created on the {@link Broker}.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code channel != null && !channel.isEmpty()}
     * pre  {@code authorisedUsers != null}
     * post class invariants satisfied
     * </pre>
     * 
     * @param channel         				the channel name.
     * @param authorizedUsers 				the regular expression defining accepted client port URIs.
     * @param clientCreator   				the URI of the client port that created the {@code channel}, 
     *                        				or {@code null} if created by the {@link Broker}.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     */
    public ChannelBrokerInfos(String channel,
                              String authorizedUsers,
                              String clientCreator) {
    	// Precondition checks
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
     * Adds or replaces a subscribed {@code clientPortURI} on the channel, IF HE IS AUTHORIZED to do it.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * pre  {@code filter != null}
     * post {@code subscribers.containsKey(clientPortURI)} // == isSubscribed(clientPortURI)
     * post {@code subscribers.get(clientPortURI).equals(filter)}
     * </pre>
     * 
     * @param clientPortURI					the client needed to be subscribed to the channel.
     * @param filter						the {@code clientPortURI}'s filter of the channel.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
	 * @throws UnauthorisedClientException 	If {@code clientPortURI} is not authorized to interact with the {@code channel}.
     */
    void addOrReplaceSubscriber(String clientPortURI, MessageFilterI filter) 
    	throws UnauthorisedClientException {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (filter == null) 
    		throw new IllegalArgumentException("A filter cannot be null");
    	
    	authorizedUsersLock.readLock().lock();
    	try {
    		if (!isAuthorized(clientPortURI)) { // takes again the authorizedUsersLock READ lock, so it is okay
    			throw new UnauthorisedClientException("Client "+clientPortURI+" is not authorized.");
    		}
    		
        	subscribersLock.writeLock().lock();
        	try {
        		
        		subscribers.put(clientPortURI, filter);
        		
        		// Postcondition checks
        		assert subscribers.containsKey(clientPortURI); // subscriber present
        		assert subscribers.get(clientPortURI).equals(filter); // with the given filter
        		
        	} finally {
        		subscribersLock.writeLock().unlock();
        	}
    	} finally {
    		authorizedUsersLock.readLock().unlock();
    	}
    }
    
    /**
     * Replaces a subscribed {@code clientPortURI} on the channel, IF HE IS SUBSCRIBED AND AUTHORIZED to do it.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * pre  {@code filter != null}
     * post {@code subscribers.containsKey(clientPortURI)} // == isSubscribed(clientPortURI)
     * post {@code subscribers.get(clientPortURI).equals(filter)}
     * </pre>
     * 
     * @param clientPortURI						the client needed to be subscribed to the channel.
     * @param filter							the {@code clientPortURI}'s filter of the channel.
     * @throws IllegalArgumentException			if a precondition is not satisfied.
	 * @throws UnauthorisedClientException 		If {@code clientPortURI} is not authorized to interact with the {@code channel}.
	 * @throws NotSubscribedChannelException    If {@code receptionPortURI} is not subscribed to the {@code channel}.
     */
    void replaceSubscriber(String clientPortURI, MessageFilterI filter) 
    	throws UnauthorisedClientException, NotSubscribedChannelException {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (filter == null) 
    		throw new IllegalArgumentException("A filter cannot be null");
    	
    	authorizedUsersLock.readLock().lock();
        try {
            if (!isAuthorized(clientPortURI)) { // takes again the authorizedUsersLock READ lock, so it is okay
                throw new UnauthorisedClientException("Client "+clientPortURI+" is not authorized.");
            }

            subscribersLock.writeLock().lock();
            try {
                if (!subscribers.containsKey(clientPortURI)) {
                    throw new NotSubscribedChannelException("Client "+clientPortURI+" is not subscribed.");
                }

                subscribers.put(clientPortURI, filter);
                
        		// Postcondition checks
        		assert subscribers.containsKey(clientPortURI); // == isSubscribed(clientPortURI), subscriber present
        		assert subscribers.get(clientPortURI).equals(filter); // with the given filter
            } finally {
                subscribersLock.writeLock().unlock();
            }
        } finally {
            authorizedUsersLock.readLock().unlock();
        }
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
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     */
    boolean isSubscribed(String clientPortURI) {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
    	subscribersLock.readLock().lock();
    	try {
    		return subscribers.containsKey(clientPortURI);
    	} finally {
    		subscribersLock.readLock().unlock();
    	}
    }

    /**
     * Removes a subscribed {@code clientPortURI}.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * pre  {@code subscribers.containsKey(clientPortURI)} // == isSubscribed(clientPortURI)
     * post {@code !subscribers.containsKey(clientPortURI)} // == !isSubscribed(clientPortURI)
     * </pre>
     * 
     * @param clientPortURI							the client to unsubscribe from the channel.
     * @throws NotSubscribedChannelException		if {@code clientPortURI} is not subscribed to the {@code channel}.
     * 
     */
    void removeSubscriber(String clientPortURI) throws NotSubscribedChannelException {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
    	subscribersLock.writeLock().lock();
    	try {
    		if (!subscribers.containsKey(clientPortURI))
    			throw new NotSubscribedChannelException("Not subscribed to '"+channel+"' channel.");
    	
    		subscribers.remove(clientPortURI);   
    		
    		// Postcondition check
    		assert (!subscribers.containsKey(clientPortURI));
    	} finally {
    		subscribersLock.writeLock().unlock();
    	}
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
    List<String> getSubscribers() {
        List<String> subs = new ArrayList<>();
        
    	subscribersLock.readLock().lock();
    	try {
	        for (Map.Entry<String, MessageFilterI> e : subscribers.entrySet()) {
	        	String subscriber = e.getKey();
	        	subs.add(subscriber);
	        }
    	} finally {
    		subscribersLock.readLock().unlock();
    	}
        
        return subs;
        // Postcondition verified by construction
    }

    // -------------------------------------------------------------------------
    // AUTHORIZED USERS MANAGEMENT
    // -------------------------------------------------------------------------

    /**
     * Updates the regular expression defining authorized users. Only the channel creator can do it.
     * 
     * <p>Subscribed users that no longer match are automatically unsubscribed.</p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code clientPortURI != null}
     * pre  {@code authorizedUsers != null}
     * post {@code this.authorizedUsers == authorizedUsers)}
     * post {@code subscribers.keySet().stream().allMatch(subscriber -> subscriber.matches(authorizedUsers))}
     * </pre>
     * 
     * @param clientPortURI 				the client wanting to modify authorized users.
     * @param authorizedUsers 				the new regular expression defining authorized users.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
	 * @throws UnauthorisedClientException  if the {@code clientPortURI} is not the creator of the {@code channel}.
     */
    void modifyAuthorizedUsers(String clientPortURI, String authorizedUsers) 
    		throws UnauthorisedClientException {
    	// Precondition check
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (authorizedUsers == null) 
    		throw new IllegalArgumentException("Authorized users cannot be null");
    	
    	if (!isCreator(clientPortURI)) {
    		throw new UnauthorisedClientException("Client "+clientPortURI+" is not the creator of the channel.");
    	}
    	
    	authorizedUsersLock.writeLock().lock(); // this lock needs to be kept until the end for postcondition
        try {
        	this.authorizedUsers = authorizedUsers;
        	
	    	List<String> subscribersToRemove = new ArrayList<>();
	    	
	    	subscribersLock.writeLock().lock();
	        try {
	        	// First, we identify subscribers who don't longer match to avoid modifying the map during iteration
	            for (Map.Entry<String, MessageFilterI> e : subscribers.entrySet()) {
	            	String subscriber = e.getKey();
	            	if (!subscriber.matches(authorizedUsers)) {
	            		subscribersToRemove.add(subscriber);
	        		}
	            }
	            
	            // Then we remove subscribers that are no longer authorized
	            for (String subscriber : subscribersToRemove) {
	            	subscribers.remove(subscriber);
	            }

		    	// Postcondition check
		    	assert subscribers.keySet().stream().allMatch(subscriber -> subscriber.matches(authorizedUsers));
    		} finally {
    			subscribersLock.writeLock().unlock();
    		}
    	} finally {
    		authorizedUsersLock.writeLock().unlock();
    	}
    }

    /**
     * Indicates if the given {@code clientPortURI} is authorized to interact with the channel.
     * 
     * <p>
     * If he is the creator, then he is automatically authorized, otherwise we verify if {@code clientPortURI}
     * matches the {@code this.authorizedUsers} regex.
     * </p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre   {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * post  {@code true} // no postcondition
     * </pre>
     * 
     * @param clientPortURI					the client for whom we verify its channel usage authorization.
     * @return 								true if {@code clientPortURI} is authorized to interact with the channel, otherwise False.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     */
    boolean isAuthorized(String clientPortURI) {
    	// Precondition check
    	if (clientPortURI == null || clientPortURI.isEmpty()) 
    		throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
    	if (isCreator(clientPortURI)) {
    		return true;
    	}
    	
    	authorizedUsersLock.readLock().lock();
        try {
    		return clientPortURI.matches(authorizedUsers);
    	} finally {
    		authorizedUsersLock.readLock().unlock();
    	}
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
     * post  {@code return >= NB_MAX_BATCH_CHANNEL)}
     * </pre>
     * 
     * @param clientsInfos					a mapping client inbound port (URI) -> its informations.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     * @throws IllegalArgumentException 	the creator registration class is unknown.
     * @return 								the maximum number of messages per batch sent from the channel.
     */
    public int getNbMaxBatch(Map<String, ClientBrokerInfos> clientsInfos) {
    	// Precondition check
    	if (clientsInfos == null) {
    		throw new IllegalArgumentException("clientsInfos cannot be null");
    	}
    	
    	// If the creator is the broker, or he has been unregistered (not in clientsInfos)
    	if (clientCreator == null || !clientsInfos.containsKey(clientCreator)) {
    		return NB_MAX_BATCH_CHANNEL;
    	}
    	
    	RegistrationClass rc = clientsInfos.get(clientCreator).getRegistrationClass();
    	switch (rc) {
    		case FREE:
    			return NB_MAX_BATCH_CHANNEL;
    		case STANDARD:
    			return NB_MAX_BATCH_STD_CHANNEL;
    		case PREMIUM:
    			return NB_MAX_BATCH_PREM_CHANNEL;
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
     * post  {@code true} 	// no postcondition
     * </pre>
     * 
     * @param clientPortURI					the client for whom we want to know if he is the channel creator.
     * @return 								true if {@code clientPortURI.equals(clientCreator)}, otherwise False.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
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
    // MESSAGES MANAGEMENT (SHARED BETWEEN TASKS AND BROKER)
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
     * @param msg							a message to load into the channel.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     */
    public void loadMessage(MessageI msg) {
    	// Precondition check
    	if (msg == null) throw new IllegalArgumentException("A message cannot be null");
        
    	synchronized (messages) {
    		messages.add(msg);
            
            // Postcondition check
            assert messages.contains(msg);
    	}
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
    public MessageI unloadMessage() {
    	synchronized (messages) {
	        return messages.poll();
    	}
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
     * @param msg							a message for which we will get users subscribed to the channel accepting it.
     * @return								the list of subscriber URIs that accept the given message.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     */
    public List<String> targetSubscribers(MessageI msg) {
    	// Precondition check
    	if (msg == null) throw new IllegalArgumentException("A message cannot be null");

    	List<String> targets = new ArrayList<>();
    	
    	subscribersLock.readLock().lock();
    	try {
	    	for (Map.Entry<String, MessageFilterI> e : subscribers.entrySet()) {
	    		String subscriber = e.getKey();
	    		MessageFilterI filter = e.getValue();
	    		if (filter.match(msg)) {
	    			targets.add(subscriber);
	    		}
	    	}
    	} finally {
    		subscribersLock.readLock().unlock();
    	}
    	
        return targets;
        // Postcondition verified by construction
    }
    
    // -------------------------------------------------------------------------
    // UNLOADING MANAGEMENT
    // -------------------------------------------------------------------------
    
    /**
     * Indicates if it is possible to create a new {@link UnloaderTask} for the current channel.
     * 
     * <p>
     * 	If so, increments the number of active unloaders for the channel, and requires that
     * 	a new {@link UnloaderTask} is created for unloading the channel.
     * </p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code nbMaxUnloadingThreads > 0}
     * post  {@code this.nbActiveUnloaders++}
     * </pre>
     * 
     * @param nbMaxUnloadingThreads 	the maximum number of unloading threads on the {@link Broker}.
     * @return 							true it is possible to create a new {@link UnloaderTask} for the current channel, otherwise false.
     * @see LoaderTask
     */
    public boolean tryNewUnloader(int nbMaxUnloadingThreads) {
    	nbActiveUnloadersLock.lock();
    	try {
            if (nbActiveUnloaders < (double) nbMaxUnloadingThreads / MAX_ACTIVE_UNLOADERS_RATIO) {
            	nbActiveUnloaders++;
                return true;
            }
            return false;
        } finally {
        	nbActiveUnloadersLock.unlock();
        }
    }
    
    /**
     * Signals to the channel that a {@link UnloaderTask} has been done, by decrementing the number of channel active unloaders.
     * 
     * <p>
     * 	An assertion is violated as a postcondition if the number of channel active unloaders becomes strictly negative.
     * 	(An Unloader has been done without incrementing the counter before)
     * </p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code tryNewUnloader()} called once before // informal
     * post  {@code this.nbActiveUnloaders >= 0}
     * </pre>
     * 
     * @see LoaderTask
     */
    public void unloaderDone() {
    	nbActiveUnloadersLock.lock();
    	try {
    		nbActiveUnloaders--;
    		
    		// Postcondition check
            assert (nbActiveUnloaders >= 0);
            // Thrown when an Unloader has been done without incrementing the counter before (negative nbActiveUnloaders)
        } finally {
        	nbActiveUnloadersLock.unlock();
        }
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