package fr.sorbonne_u.broker.components.infos;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.components.infos.exceptions.ChannelNumberNotInitializedException;
import fr.sorbonne_u.broker.ports.BrokerReceivingOutboundPort;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.cps.pubsub.exceptions.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Class storing all information about a {@link Client} registered on the {@link Broker}.
 * 
 * <p>
 * Before creating an instance, the {@link Broker} must initialize static attributes {@code nbMaxChannelsStd} and {@code nbMaxChannelsPrem}.
 * </p>
 * 
 * <p>
 * In case of multiple threads accessing on the same instance informations that can be modified, a global lock is used :
 * because of having only short operations, it does not cost a lot in terms of performances.
 * </p>
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
 *   <li>The {@link BrokerReceivingOutboundPort}, outbound port towards this client. {@code null} if the client is not in the same JVM as the Broker.</li>
 *   <li>The number of remaining channels the client can create. When one its channels is destroyed, the number is increased.</li>
 * </ul>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code clientPortURI != null && !clientPortURI.isEmpty()}
 * invariant	{@code rc != null}
 * invariant	{@code nbChannelsRemaining >= 0}
 * invariant	{@code rcLock != null}
 * invariant	{@code nbChannelsRemainingLock != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientBrokerInfos {
	
	/** The maximum number of channels that a standard client can create. Set by the Broker in its constructor */
	public static int nbMaxChannelsStd = -1;
	/** The maximum number of channels that a premium client can create. Set by the Broker in its constructor */
	public static int nbMaxChannelsPrem = -1;

	/** The URI of the client port, defining the client */
	private final String clientPortURI;
	
	/** The registration class of the client */
	private RegistrationClass rc;
	
	/** The outbound port towards this client. {@code null} if the client is not in the same JVM as the Broker. */
	private final BrokerReceivingOutboundPort clientOutboundPort;
	
	/** The number of remaining channels the client can create */
	private int nbRemainingChannels;
	
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
	 */
	public ClientBrokerInfos(String clientPortURI, RegistrationClass rc, BrokerReceivingOutboundPort clientOutboundPort) {
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
		default:
			this.nbRemainingChannels = nbMaxChannelsPrem;
			break;
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
	public synchronized RegistrationClass getRegistrationClass() {
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
	public synchronized void setRegistrationClass(RegistrationClass rc) {
		// Precondition check
		if (rc == null) throw new IllegalArgumentException("A registration class cannot be null");
		
		this.rc = rc;
		// Postcondition verified by construction
	}
	
	// -------------------------------------------------------------------------
	// OUTBOUND PORT MANAGEMENT
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the {@link BrokerReceivingOutboundPort} associated with the client.
	 * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} // no precondition
     * post {@code != null}
     * </pre>
	 * 
	 * @return the client's {@link BrokerReceivingOutboundPort}.
	 */
	public BrokerReceivingOutboundPort getOutboundPort() {
		return clientOutboundPort; // clientOutboundPort cannot not be modified, so no need to protect it from a data race
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
	public synchronized boolean canCreateChannel() {
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
	public synchronized void decreaseRemainingChannels() throws ChannelQuotaExceededException{
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
	public synchronized void increaseRemainingChannels() throws ChannelQuotaExceededException{
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