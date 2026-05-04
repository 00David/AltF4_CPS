package fr.sorbonne_u.clients.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.RegistrationServiceConnector;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Delegates the registration to its {@link RegistrationServiceConnector}.
 * 
 * <p>
 * Used as an outbound port of a {@link Client}, 
 * allowing the registration to a {@link Broker}.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class RegistrationOutboundPort extends AbstractOutboundPort implements RegistrationCI {

	private static final long serialVersionUID = 674403752614646152L;

	/**
	 * Creates the outbound port redirecting to a {@link RegistrationServiceConnector}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code owner != null}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param owner  		the component owning this port.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public RegistrationOutboundPort(ComponentI owner) throws Exception {
		super(RegistrationCI.class,owner);
		// Precondition check
		assert(owner != null) : new PreconditionException("owner cannot be null");
	}
	
	// Precondition and postcondition verifications are delegated to the RegistrationServiceConnector

	@Override
	public boolean registered(String receptionPortURI) throws Exception {
		return ((RegistrationCI)this.getConnector()).registered(receptionPortURI);
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.getConnector()).registered(receptionPortURI, rc);
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.getConnector()).register(receptionPortURI, rc);
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.getConnector()).modifyServiceClass(receptionPortURI, rc);
	}

	@Override
	public void unregister(String receptionPortURI) throws Exception {
		((RegistrationCI)this.getConnector()).unregister(receptionPortURI);
		
	}

	@Override
	public boolean channelExist(String channel) throws Exception {
		return ((RegistrationCI)this.getConnector()).channelExist(channel);
	}
	
	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws Exception {
		return ((RegistrationCI)this.getConnector()).channelAuthorised(receptionPortURI, channel);
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		return ((RegistrationCI)this.getConnector()).subscribed(receptionPortURI, channel);
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		((RegistrationCI)this.getConnector()).subscribe(receptionPortURI, channel, filter);
		
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		((RegistrationCI)this.getConnector()).unsubscribe(receptionPortURI, channel);
		
	}

	@Override
	public void modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		((RegistrationCI)this.getConnector()).modifyFilter(receptionPortURI, channel, filter);
	}
}