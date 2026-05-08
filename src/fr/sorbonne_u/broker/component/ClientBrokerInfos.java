package fr.sorbonne_u.broker.component;

import fr.sorbonne_u.broker.component.exceptions.ChannelNumberNotInitializedException;
import fr.sorbonne_u.broker.ports.ReceivingOutboundPort;
import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.cps.pubsub.exceptions.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Class storing all information about a {@link Client} registered on the {@link Broker}.
 * 
 * <p>
 * Before creating an instance, the {@link Broker} must initialize static attributes {@code nbMaxChannelsStd} and {@code nbMaxChannelsPrem}.
 * </p>
 * 
 * <p>
 * In case of multiple threads accessing to the same instance, the <strong>synchronized monitor lock</strong> is used on it 
 * (except while using the client port for sending him messages) : 
 * </p>
 * <ul>
 * 	<li>Having only short operations ensures that it does not cost a lot in terms of system performances
 * 		(=time while the client is locked).</li>
 * 	<li>In the {@link Broker}, this client lock is only taken while doing operations that are NOT publishing,
 * 		and thus threads keep the lock during a reasonable time. Using its reentrant property also allows a thread
 * 		in the broker to take the client lock, and do multiple operations on the client while keeping it (=critical section).</li>
 * 	<li>On the opposite of a channel, a client will be in general accessed less frequently by multiple threads simultaneously,
 * 		so it can be less relevant having a lock for each internal resource.</li>
 * </ul>
 * 
 * <p>
 * Each instance is uniquely identified by its {@code clientPortURI} attribute.
 * </p>
 * 
 * <p>
 * Maintained information includes :
 * </p>
 * <ul>
 *   <li>The URI of the client port.</li>
 *   <li>The {@link RegistrationClass} of the client.</li>
 *   <li>The {@link ReceivingOutboundPort} towards this client. {@code null} if the client is not in the same JVM as the Broker.</li>
 *   <li>The number of remaining channels the client can create. When one of its channels is destroyed, the number is increased.</li>
 * </ul>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code clientPortURI != null && !clientPortURI.isEmpty()}
 * invariant	{@code rc != null}
 * invariant	{@code nbChannelsRemaining >= 0}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientBrokerInfos {
	
    // -------------------------------------------------------------------------
    // STATIC VARIABLES (SET IN THE BROKER CONSTRUCTOR)
    // -------------------------------------------------------------------------
	
	/** The maximum number of channels that a standard client can create. Set by the Broker in its constructor. */
	public static int nbMaxChannelsStd = -1;
	/** The maximum number of channels that a premium client can create. Set by the Broker in its constructor. */
	public static int nbMaxChannelsPrem = -1;
	
    // -------------------------------------------------------------------------
    // INTERNAL DATAS
    // -------------------------------------------------------------------------

	/** The URI of the client port, defining the client. */
	private final String clientPortURI;
	
	/** The registration class of the client. */
	private RegistrationClass rc;
	
	/** 
	 * The outbound port towards this client. {@code null} if the client is not in the same JVM as the Broker.
	 * <p>If the client is subscribed to multiple channels, this same port will always be used for sending him messages.</p>.
	 */
	private final ReceivingOutboundPort clientOutboundPort;
	
	/** The number of remaining channels the client can create. */
	private int nbRemainingChannels;
	
    // -------------------------------------------------------------------------
    // CONSTRUCTOR
    // -------------------------------------------------------------------------
	
	/**
	 * Creates the information structure for a {@link Client} registered on the {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
	 * pre  {@code rc != null}
	 * pre  {@code nbMaxChannelsStd >= 0}
	 * pre  {@code nbMaxChannelsPrem >= 0}
	 * post class invariants satisfied
	 * </pre>
	 * 
	 * @param clientPortURI         				the URI of the client's inbound port offering the {@code ReceivingCI} component interface.
	 * @param rc                    				the registration class of the client.
	 * @param clientOutboundPort    				the outbound port of the {@link Broker} requiring the {@code ReceivingCI} interface, associated with the client. 
	 * 												{@code null} if the client is not in the same JVM as the Broker.
	 * @throws IllegalArgumentException 			if a precondition is not satisfied, except for {@code nbMaxChannelsStd} and {@code nbMaxChannelsPrem}.
	 * @throws ChannelNumberNotInitializedException	if {@code nbMaxChannelsStd} or {@code nbMaxChannelsPrem} have not yet been initialized by the {@link Broker}.
	 * @throws IllegalArgumentException 			if the given {@code rc} has no switch case here.
	 */
	public ClientBrokerInfos(String clientPortURI, RegistrationClass rc, ReceivingOutboundPort clientOutboundPort) {
		if (clientPortURI == null || clientPortURI.isEmpty()) 
			throw new IllegalArgumentException("A client port URI cannot be null or empty");
		if (rc == null) 
			throw new IllegalArgumentException("A registration class cannot be null");
		
		if (nbMaxChannelsStd < 0) 
			throw new ChannelNumberNotInitializedException("Number of max channels for standard clients cannot be negative");
		if (nbMaxChannelsPrem < 0) 
			throw new ChannelNumberNotInitializedException("Number of max channels for premium clients cannot be negative");
		
		this.clientPortURI = clientPortURI;
		this.rc = rc;
		this.clientOutboundPort = clientOutboundPort;
		
		switch(rc) {
		case FREE:
			this.nbRemainingChannels = 0;
			break;
		case STANDARD:
			this.nbRemainingChannels = nbMaxChannelsStd;
			break;
		case PREMIUM:
			this.nbRemainingChannels = nbMaxChannelsPrem;
			break;
		default:
			throw new IllegalArgumentException("Undefined registration class.");
		}
	}
	
	// -------------------------------------------------------------------------
	// REGISTRATION CLASS MANAGEMENT
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the {@link RegistrationClass} of the client.
	 * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} // no precondition
     * post {@code return != null}
     * </pre>
	 * 
	 * @return the client's {@link RegistrationClass}.
	 */
	synchronized RegistrationClass getRegistrationClass() {
		return rc;
	}
	
	/**
	 * Replaces the {@link RegistrationClass} of the client.
	 * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code rc != null}
     * post {@code this.rc = rc}
     * </pre>
	 * 
	 * @param rc 						the new {@link RegistrationClass} of the client.
	 * @throws IllegalArgumentException if {@code rc} is null.
	 */
	synchronized void setRegistrationClass(RegistrationClass rc) {
		// Precondition check
		if (rc == null) throw new IllegalArgumentException("A registration class cannot be null");
		
		this.rc = rc;
		// Postcondition verified by construction
	}
	
	// -------------------------------------------------------------------------
	// OUTBOUND PORT MANAGEMENT (PACKAGE VISIBILITY FOR SAFETY)
	// -------------------------------------------------------------------------
	
	/**
	 * Send {@code messages} from {@code channel}, by delegating to the corresponding client
	 * {@link ReceivingOutboundPort} IF the client is on the same JVM (this.clientOutboundPort != null).
	 * 
	 * <p>Only method NOT covered by the SYNCHRONIZED monitor.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.size() > 0}
	 * pre	{@code Stream.of(messages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel		name of the channel from which {@code messsages} are received.
	 * @param messages		array of messages received by the target client component.
	 * @return				true if {@code messages} have been sent, otherwise false.
	 * @throws Exception	if an error occurs while sending messages.
	 */
	boolean sendMessages(String channel, MessageI[] messages) throws Exception {
		// Preconditions are delegated to the ReceivingOutboundPort receive method
		
		if (clientOutboundPort != null) {
			clientOutboundPort.receive(channel, messages);
			return true;
		}
		return false;
	}
	
	/**
	 * Disconnect the associated client {@link ReceivingOutboundPort} 
	 * IF the client is on the same JVM (this.clientOutboundPort != null).
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * post	clientOutboundPort not connected // informal
	 * </pre>
	 *
	 * @param owner							the {@link Broker} handling the client, and thus owning its port.
	 * @return								true if the port has been disconnected, otherwise false.
     * @throws IllegalArgumentException		if a precondition is not satisfied.
	 * @throws Exception					if an error occurs while disconnecting the port.
	 */
	synchronized boolean disconnectClientPort(Broker owner) throws Exception {
		// Precondition check
		if (owner == null)
			throw new IllegalArgumentException("Broker owner cannot be null.");
		
		if (clientOutboundPort != null) {
			owner.doPortDisconnection(clientOutboundPort.getPortURI());
			return true;
		}
		return false;
	}
	
	/**
	 * Unpublish the associated client {@link ReceivingOutboundPort} 
	 * IF the client is on the same JVM (this.clientOutboundPort != null).
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	clientOutboundPort not published // informal
	 * </pre>
	 *
	 * @return				true if the port has been disconnected, otherwise false.
	 * @throws Exception	if an error occurs while unpublishing the port.
	 */
	synchronized boolean unpublishClientPort() throws Exception {
		if (clientOutboundPort != null) {
			clientOutboundPort.unpublishPort();
			return true;
		}
		return false;
	}
	
	// -------------------------------------------------------------------------
	// REMAINING CHANNELS MANAGEMENT
	// -------------------------------------------------------------------------
	
	/**
	 * Indicates whether the client can create at least one channel.
	 * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} // no precondition
     * post {@code true} // no postcondition
     * </pre>
	 * 
	 * @return true if at least one channel can be created by the client, false otherwise.
	 */
	synchronized boolean canCreateChannel() {
		return nbRemainingChannels > 0;
	}
	
	/**
	 * Decreases the number of channels the client can still create.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code this.nbRemainingChannels > 0}
	 * post {@code this.nbRemainingChannels >= 0}
	 * </pre>
	 * 
	 * @throws ChannelQuotaExceededException	if {@code this.nbChannelsRemaining} would become negative.
	 */
	synchronized void decreaseRemainingChannels() throws ChannelQuotaExceededException{
		// Precondition check
		if (nbRemainingChannels == 0) 
			throw new ChannelQuotaExceededException("The number of remaining channels cannot become negative");
			
		nbRemainingChannels--;
			
		// Postcondition check
		assert(nbRemainingChannels >= 0);
	}
	
	/**
	 * Increases the number of channels the client can still create, if the client is not FREE.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
     * pre  {@code this.rc != RegistrationClass.FREE}
     * post {@code true} // no postcondition
	 * </pre>
	 * 
	 * @throws ChannelQuotaExceededException	if {@code this.nbChannelsRemaining} would become strictly positive for a FREE client.
	 */
	synchronized void increaseRemainingChannels() throws ChannelQuotaExceededException{
		// Precondition check
		if (rc == RegistrationClass.FREE) 
			throw new ChannelQuotaExceededException("A free client cannot increase its number of remaining channels (0 by default)");
		
		nbRemainingChannels++;
	}
	
	// A ClientBrokerInfos is uniquely identified by the client's port URI
	// => override equals and hashCode accordingly
	
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientBrokerInfos that = (ClientBrokerInfos) o;
        return clientPortURI.equals(that.clientPortURI);
    }

    @Override
    public int hashCode() { 
    	return clientPortURI.hashCode(); 
    }
}