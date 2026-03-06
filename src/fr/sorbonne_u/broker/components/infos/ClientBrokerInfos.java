package fr.sorbonne_u.broker.components.infos;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.components.infos.exceptions.ChannelNumberNotInitializedException;
import fr.sorbonne_u.broker.components.infos.exceptions.NegativeRemainingChannelsException;
import fr.sorbonne_u.broker.components.infos.exceptions.CannotIncreaseRemainingChannelsException;
import fr.sorbonne_u.broker.ports.BrokerReceivingOutboundPort;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Class storing all information about a {@link Client} registered on the {@link Broker}.
 * 
 * <p>
 * Before creating an instance, the {@link Broker} must initialize static attributes {@code nbMaxChannelsStd} and {@code nbMaxChannelsPrem}.
 * </p>
 * 
 * <p>
 * Each instance is uniquely identified by its {@code clientPortURI} attribute.
 * </p>
 * 
 * <p>
 * Maintained information includes:
 * <ul>
 *   <li>The URI of the client port.</li>
 *   <li>The {@link RegistrationClass} of the client.</li>
 *   <li>The {@link BrokerReceivingOutboundPort}, outbound port towards this client.</li>
 *   <li>The number of remaining channels the client can create. When one its channels is destroyed, the number is increased.</li>
 * </ul>
 * </p>
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
	
	// Set by the Broker in its constructor
	public static int nbMaxChannelsStd = -1;
	public static int nbMaxChannelsPrem = -1;

	private final String clientPortURI;
	private RegistrationClass rc;
	private final BrokerReceivingOutboundPort clientOutboundPort;
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
	 * @throws IllegalArgumentException 			if a precondition is not satisfied, except for {@code nbMaxChannelsStd} and {@code nbMaxChannelsPrem}.
	 * @throws ChannelNumberNotInitializedException	if {@code nbMaxChannelsStd} or {@code nbMaxChannelsPrem} have not yet been initialized by the {@link Broker}.
	 */
	public ClientBrokerInfos(String clientPortURI, RegistrationClass rc, BrokerReceivingOutboundPort clientOutboundPort) {
		if (clientPortURI == null || clientPortURI.isEmpty()) throw new IllegalArgumentException("A client port URI cannot be null or empty");
		if (rc == null) throw new IllegalArgumentException("A registration class cannot be null");
		
		if (nbMaxChannelsStd < 0) throw new ChannelNumberNotInitializedException("Number of max channels for standard clients cannot be negative");
		if (nbMaxChannelsPrem < 0) throw new ChannelNumberNotInitializedException("Number of max channels for premium clients cannot be negative");
		
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
	
	// Getters postconditions verified by class invariants
	
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
	public RegistrationClass getRegistrationClass() {
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
	public void setRegistrationClass(RegistrationClass rc) {
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
     * post {@code true} // no postcondition
     * </pre>
	 * 
	 * @return the client's {@link BrokerReceivingOutboundPort}.
	 */
	public BrokerReceivingOutboundPort getOutboundPort() {
		return clientOutboundPort;
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
	public boolean canCreateChannel() {
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
	 * @throws NegativeRemainingChannelsException	if {@code this.nbChannelsRemaining} would become negative.
	 */
	public void decreaseRemainingChannels() {
		// Precondition check
		if (nbRemainingChannels == 0) throw new NegativeRemainingChannelsException("The number of remaining channels cannot become negative");
		
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
	 * @throws CannotIncreaseRemainingChannelsException	if {@code this.nbChannelsRemaining} would become strictly positive for a FREE client.
	 */
	public void increaseRemainingChannels() {
		// Precondition check
		if (rc == RegistrationClass.FREE) throw new CannotIncreaseRemainingChannelsException("A free client cannot have more than 0 channels to create");
		
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