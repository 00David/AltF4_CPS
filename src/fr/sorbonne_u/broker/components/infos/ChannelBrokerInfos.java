package fr.sorbonne_u.broker.components.infos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * Class storing all information related to an existing channel on the {@link Broker}.
 * 
 * <p>
 * Each instance is uniquely identified by its {@code channel} attribute.
 * </p>
 * 
 * <p>
 * Maintained information includes:
 * <ul>
 * 	<li> - The channel name.
 * 	<li> - A set of pairs: (subscriber client port + channel filter).
 * 	<li> - Whether the channel is accessible only to privileged clients or to everyone (default channel).
 * 	<li> - The regular expression defining the accepted client port URIs for this channel.
 * 	<li> - The client port that created the channel, or {@code null} if it was created by the {@link Broker}.
 * 	<li> - A FIFO queue of {@link MessageI} pending on the channel (to be sent).
 * </ul>
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code channel != null && !channel.isEmpty()}
 * invariant	{@code subscribers != null}
 * invariant	{@code authorisedUsers != null}
 * invariant	{@code messages != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ChannelBrokerInfos {

    /**
     * Pair: subscriber client port + channel filter.
     * 
     * <p>
     * Each instance is uniquely identified by its {@code subscriberPortURI}.
     * </p>
     * 
     * <p><strong>Invariants</strong></p>
     * <pre>
     * invariant	{@code subscriberPortURI != null && !subscriberPortURI.isEmpty()}
     * </pre>
     * 
     * @author Kevin Tang
     * @author David Vadimon
     */
    private static final class SubsciberFilter {
        private final String subscriberPortURI;
        private final MessageFilterI filter;

        /**
         * Creates a pair: subscriber client port + channel filter.
         * 
         * <p><strong>Contract</strong></p>
         * <pre>
         * pre  {@code subscriberPortURI != null && !subscriberPortURI.isEmpty()}
         * post internal class invariants satisfied
         * </pre>
         * 
         * @param subscriberPortURI 		the URI of the client inbound port implementing {@code ReceivingCI}.
         * @param filter            		the client's filter for the channel.
         * @throws IllegalArgumentException if a {@code subscriberPortURI} is null or empty.
         */
        private SubsciberFilter(String subscriberPortURI, MessageFilterI filter) {
        	if (subscriberPortURI == null || subscriberPortURI.isEmpty()) 
        		throw new IllegalArgumentException("Subsciber URI cannot be null or empty");
            this.subscriberPortURI = subscriberPortURI;
            this.filter = filter;
        }

        // Equality based solely on subscriberPortURI

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SubsciberFilter that = (SubsciberFilter) o;
            return subscriberPortURI.equals(that.subscriberPortURI);
        }

        @Override
        public int hashCode() {
            return subscriberPortURI.hashCode();
        }
    }

    private final String channel;
    private final Set<SubsciberFilter> subscribers;
    private final boolean onlyPrivileged;
    private String authorisedUsers;
    private final String clientCreator;
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
     * @param authorisedUsers the regular expression defining accepted client port URIs.
     * @param clientCreator   the URI of the client port that created the {@code channel},
     *                        or {@code null} if created by the {@link Broker}.
     * @param onlyPrivileged  true if accessible only to privileged clients, false otherwise.
     */
    public ChannelBrokerInfos(String channel,
                              String authorisedUsers,
                              String clientCreator,
                              boolean onlyPrivileged) {
    	
    	if (channel == null || channel.isEmpty()) 
    		throw new IllegalArgumentException("A channel name cannot be null or empty");
        if (authorisedUsers == null) throw new IllegalArgumentException("The regex for authorised users cannot be null");

        this.channel = channel;
        this.subscribers = new HashSet<>();
        this.onlyPrivileged = onlyPrivileged;
        this.authorisedUsers = authorisedUsers;
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
     * </pre>
     * 
     * @param clientPortURI					the client needed to be subscibed to the channel.
     * @param filter						the {@code clientPortURI}'s filter fo the channel.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     */
    public void addOrReplaceSubscriber(String clientPortURI, MessageFilterI filter) {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (filter == null) throw new IllegalArgumentException("A filter cannot be null");

        subscribers.add(new SubsciberFilter(clientPortURI, filter));
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
    public boolean isSubscribed(String clientPortURI) {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	
        return subscribers.contains(new SubsciberFilter(clientPortURI, null));
    }

    /**
     * Removes a subscribed {@code clientPortURI}.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
     * pre  {@code isSubscribed(clientPortURI)}
     * post {@code isSubscribed(clientPortURI) == false}
     * </pre>
     * 
     * @param clientPortURI					the client to unsubscribe from the channel.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
     * 
     */
    public void removeSubscriber(String clientPortURI) {
    	// Precondition checks
    	if (clientPortURI == null || clientPortURI.isEmpty()) throw new IllegalArgumentException("A client port URI cannot be null or empty");
    	if (!isSubscribed(clientPortURI)) throw new IllegalArgumentException("Client not already subscribed");

        subscribers.remove(new SubsciberFilter(clientPortURI, null));
        
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
    public List<String> getSubscribers() {
        List<String> subs = new ArrayList<>();
        for (SubsciberFilter sub : subscribers) {
            subs.add(sub.subscriberPortURI);
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
        return onlyPrivileged;
    }

    /**
     * Updates the regular expression defining authorized users.
     * 
     * <p>Subscribed users that no longer match are automatically unsubscribed.</p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code authorizedUsers != null}
     * post  {@code this.authorizedUsers == authorizedUsers)}
     * post  {@code subscribers.stream().allMatch(sub -> sub.subscriberPortURI.matches(this.authorisedUsers))}
     * </pre>
     * 
     * @throws IllegalArgumentException if {@code authorizedUsers} is null.
     */
    public void modifyAuthorizedUsers(String authorizedUsers) {
    	// Precondition check
    	if (authorisedUsers == null) throw new IllegalArgumentException("Authorized users cannot be null");
    	
        this.authorisedUsers = authorizedUsers;

        List<String> currentSubs = getSubscribers();
        for (String sub : currentSubs) {
            if (!sub.matches(authorizedUsers)) {
                removeSubscriber(sub);
            }
        }
        
        // Postcondition check
        assert subscribers.stream().allMatch(sub -> sub.subscriberPortURI.matches(this.authorisedUsers));
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
    public String getAuthorizedUsers() {
        return authorisedUsers;
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
        return clientCreator;
    }

    // -------------------------------------------------------------------------
    // MESSAGES MANAGEMENT
    // -------------------------------------------------------------------------

    /**
     * Adds a {@link MessageI} to the channel.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code msg != null}
     * post  {@code this.messages.contains(msg)}
     * </pre>
     * 
     * @throws IllegalArgumentException if the {@code msg} is null.
     */
    public void addMessage(MessageI msg) {
    	// Precondition check
    	if (msg == null) throw new IllegalArgumentException("A message cannot be null");
        
        messages.add(msg);
        
        // Postcondition check
        assert messages.contains(msg);
    }

    /**
     * Adds multiple {@link MessageI} instances to the channel.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code msgs != null && !msgs.isEmpty()}
     * post  {@code this.messages.containsAll(msgs)}
     * </pre>
     * 
     * @throws IllegalArgumentException if {@code msgs} is null or empty.
     */
    public void addMessages(List<MessageI> msgs) {
    	// Precondition checks
    	if (msgs == null || msgs.isEmpty()) throw new IllegalArgumentException("Messages cannot be null or empty");
        
        messages.addAll(msgs);
        
        // Postcondition check
        assert messages.containsAll(msgs);
    }

    /**
     * Retrieves and removes the next {@link MessageI} to process.
     * 
     * <p>It returns {@code null} if no message is available on the channel.</p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true}	// no precondition
     * post  {@code true}	// no postcondition
     * </pre>
     * 
     * @return the next {@link MessageI} to process, or {@code null} if no message is available on the channel.
     */
    public MessageI getMessage() {
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
     * @return							the list of subscriber URIs that accept the given message.
     * @throws IllegalArgumentException	if the {@code msg} is null.
     */
    public List<String> targetSubscribers(MessageI msg) {
    	// Precondition check
    	if (msg == null) throw new IllegalArgumentException("A message cannot be null");

        List<String> targets = new ArrayList<>();
        for (SubsciberFilter sub : subscribers) {
            if (sub.filter.match(msg)) {
                targets.add(sub.subscriberPortURI);
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