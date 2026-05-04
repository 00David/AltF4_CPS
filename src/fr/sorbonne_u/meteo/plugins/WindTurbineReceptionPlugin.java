package fr.sorbonne_u.meteo.plugins;

import java.io.Serializable;
import java.util.stream.Stream;

import fr.sorbonne_u.clients.plugins.ClientRegistrationPlugin;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.meteo.AlertMessage;
import fr.sorbonne_u.messages.meteo.WindMessage;
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
	 * pre	{@code registrationOutboundPort != null}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param registrationPlugin		The client registration plug-in owning an outbound port requiring {@link RegistrationCI} interface.
	 */
	public WindTurbineReceptionPlugin(ClientRegistrationPlugin registrationPlugin) {
		super(registrationPlugin);
	}
	
	// -------------------------------------------------------------------------
	// Overrides abstract Client reception services
	// -------------------------------------------------------------------------

	@Override
	public void receive(String channel, MessageI message) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(message != null) : 
			new PreconditionException("message cannot be null");
		
		WindTurbine windTurbine = (WindTurbine)this.getOwner();
		
		windTurbine.logMessage("------------------------------\n"
				+ "Thread "+Thread.currentThread().getId()+": WindTurbine received message '" + message + "' from the channel : " + channel 
				+ "\n------------------------------");
		Serializable payload = message.getPayload();

		// Handling of received wind data delegated to the WindTurbine
		if (message instanceof WindMessage && !windTurbine.inSafeMode()) {
			windTurbine.considerNewWindData((WindData) payload);
		
		// Handling of received meteo alert delegated to the WindTurbine
		} else if (message instanceof AlertMessage) {
			windTurbine.considerNewAlert((MeteoAlert) payload);
			
		// Handling of received wind data while in safe mode (nothing)
		} else if (windTurbine.inSafeMode()) {
			
		} else {
			System.err.println("Unknown received message class");
		}
	}

	@Override
	public void receive(String channel, MessageI[] messages) {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(messages != null && messages.length > 0) : 
			new PreconditionException("messages container cannot be null or empty");
		assert(Stream.of(messages).allMatch(m -> m != null)) : 
			new PreconditionException("a message received cannot be null");
		
		WindTurbine windTurbine = (WindTurbine)this.getOwner();
		
		windTurbine.logMessage("------------------------------\n"
				+ "Thread "+Thread.currentThread().getId()+": WindTurbine received "+ messages.length +" messages from the channel : " + channel 
				+ "\n------------------------------");
		for (int i=0; i<messages.length; i++) {
			
			MessageI message = messages[i];
			//windTurbine.logMessage("Message " + i + " '" + message + "'");
			Serializable payload = message.getPayload();
			
			// Handling of received wind data delegated to the WindTurbine
			if (message instanceof WindMessage && !windTurbine.inSafeMode()) {
				windTurbine.considerNewWindData((WindData) payload);
			
			// Handling of received meteo alert delegated to the WindTurbine
			} else if (message instanceof AlertMessage) {
				windTurbine.considerNewAlert((MeteoAlert) payload);
				
			// Handling of received wind data while in safe mode (nothing)
			} else if (windTurbine.inSafeMode()) {
				
			} else {
				System.err.println("Unknown received message class");
			}
		}
	}
	
}
