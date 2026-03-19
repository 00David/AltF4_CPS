package fr.sorbonne_u.meteo.plugins;

import java.io.Serializable;
import java.util.stream.Stream;

import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.clients.ports.ClientReceivingPluginInboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.WindData;

/**
 * Creates a {@link ClientSubscriptionPlugin} defining personalized {@link WindTurbine} behavior on messages reception.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true} // no more invariant
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class WindTurbineReceptionPlugin extends ClientSubscriptionPlugin {

	private static final long serialVersionUID = -6762754298089497004L;

	/**
	 * Creates a {@link WindTurbine} subscription + reception plugin.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationOutboundPort != null}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI		the wind turbine inbound port URI offering {@link ReceivingCI} interface, used for creating the {@link ClientReceivingPluginInboundPort}.
	 * @param registrationOutboundPort		the wind turbine outbound port requiring {@link RegistrationCI} interface.
	 */
	public WindTurbineReceptionPlugin(String receivingInboundPortURI,
			ClientRegistrationOutboundPort registrationOutboundPort) {
		super(receivingInboundPortURI, registrationOutboundPort);
	}
	
	// -------------------------------------------------------------------------
	// Overrides default Client reception services
	// -------------------------------------------------------------------------

	@Override
	public void receive(String channel, MessageI message) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(message != null) : new PreconditionException("message cannot be null");
		nbMessagesReceived.incrementAndGet();
		
		WindTurbine windTurbine = (WindTurbine)this.getOwner();
		
		windTurbine.logMessage("------------------------------");
		windTurbine.logMessage("WindTurbine received message '" + message + "' from the channel : " + channel);
		Serializable payload = message.getPayload();

		// Handling of received wind data delegated to the WindTurbine
		if (payload instanceof WindData && !windTurbine.inSafeMode()) {
			windTurbine.considerNewWindData((WindData) payload);
		
		// Handling of received meteo alert delegated to the WindTurbine
		} else if (payload instanceof MeteoAlert) {
			windTurbine.considerNewAlert((MeteoAlert) payload);
			
		// Handling of received wind data while in safe mode (nothing)
		} else {
			windTurbine.logMessage("------------------------------");
		}
	}

	@Override
	public void receive(String channel, MessageI[] messages) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(messages != null && messages.length > 0) : new PreconditionException("messages container cannot be null or empty");
		assert(Stream.of(messages).allMatch(m -> m != null)) : new PreconditionException("a message received cannot be null");
		nbMessagesReceived.incrementAndGet();
		
		WindTurbine windTurbine = (WindTurbine)this.getOwner();
		
		windTurbine.logMessage("------------------------------");
		windTurbine.logMessage("WindTurbine received multiple messages from the channel : " + channel);
		for (int i=0; i<messages.length; i++) {
			MessageI message = messages[i];
			windTurbine.logMessage("Message " + i + " '" + message + "'");
			Serializable payload = message.getPayload();
			
			// Handling of received wind data delegated to the WindTurbine
			if (payload instanceof WindData && !windTurbine.inSafeMode()) {
				windTurbine.considerNewWindData((WindData) payload);
			
			// Handling of received meteo alert delegated to the WindTurbine
			} else if (payload instanceof MeteoAlert) {
				windTurbine.considerNewAlert((MeteoAlert) payload);
				
			// Handling of received wind data while in safe mode (nothing)
			} else {
				windTurbine.logMessage("------------------------------");
			}
		}
	}
}
