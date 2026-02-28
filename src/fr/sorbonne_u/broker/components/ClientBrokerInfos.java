package fr.sorbonne_u.broker.components;

import fr.sorbonne_u.broker.ports.BrokerReceivingOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Class storing all information about a {@link Client} registered on the {@link Broker}.
 * 
 * <p>
 * Each instance is uniquely identified by its {@code clientPortURI} attribute.
 * </p>
 * 
 * <p>
 * Maintained information:
 * <ul>
 *   <li>The URI of the client port.</li>
 *   <li>The {@link RegistrationClass} of the client.</li>
 *   <li>The {@link BrokerReceivingOutboundPort}, outbound port towards this client.</li>
 * </ul>
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code clientPortURI != null && !clientPortURI.isEmpty()}
 * invariant	{@code rc != null}
 * invariant	{@code clientOutboundPort != null}
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
	private int nbChannelsRemaining;
	
	/**
	 * Creates the information structure for a {@link Client} registered on the {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code clientPortURI != null && !clientPortURI.isEmpty()}
	 * pre  {@code rc != null}
	 * pre  {@code clientOutboundPort != null}
	 * pre  {@code nbMaxChannelsStd >= 0}
	 * pre  {@code nbMaxChannelsPrem >= 0}
	 * post class invariants satisfied
	 * </pre>
	 * 
	 * @param clientPortURI         	the URI of the client's inbound port offering the {@code ReceivingCI} component interface.
	 * @param rc                    	the registration class of the client.
	 * @param clientOutboundPort    	the outbound port of the {@link Broker} requiring the {@code ReceivingCI} interface, associated with the client.
	 * @throws IllegalArgumentException if a precondition is not satisfied.
	 */
	public ClientBrokerInfos(String clientPortURI, RegistrationClass rc, BrokerReceivingOutboundPort clientOutboundPort) {
		if (clientPortURI == null || clientPortURI.isEmpty()) throw new IllegalArgumentException("A client port URI cannot be null or empty");
		if (rc == null) throw new IllegalArgumentException("A registration class cannot be null");
		if (clientOutboundPort == null) throw new IllegalArgumentException("A client outbound port cannot be null");
		if (nbMaxChannelsStd < 0 || nbMaxChannelsPrem < 0) throw new IllegalArgumentException("Max channels must be positive");
		
		this.clientPortURI = clientPortURI;
		this.rc = rc;
		this.clientOutboundPort = clientOutboundPort;
		
		switch(rc) {
		case FREE:
			this.nbChannelsRemaining = 0;
			break;
		case STANDARD:
			this.nbChannelsRemaining = nbMaxChannelsStd;
			break;
		default:
			this.nbChannelsRemaining = nbMaxChannelsPrem;
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
     * post {@code return != null}
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
		return nbChannelsRemaining > 0;
	}
	
	/**
	 * Decreases the number of channels the client can still create.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code this.nbChannelsRemaining > 0}
	 * post {@code this.nbChannelsRemaining >= 0}
	 * </pre>
	 */
	public void decreaseRemainingChannels() {
		// Precondition check
		assert(nbChannelsRemaining > 0);
		
		nbChannelsRemaining--;
		
		// Postcondition check
		assert(nbChannelsRemaining >= 0);
	}
	
	/**
	 * Increases the number of channels the client can still create.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
     * pre  {@code true} // no precondition
     * post {@code true} // no postcondition
	 * </pre>
	 */
	public void increaseRemainingChannels() {
		nbChannelsRemaining++;
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