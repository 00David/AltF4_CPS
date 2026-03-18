package fr.sorbonne_u.broker.components.tasks;

import java.util.List;
import java.util.Map;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.components.infos.ChannelBrokerInfos;
import fr.sorbonne_u.broker.components.infos.ClientBrokerInfos;
import fr.sorbonne_u.components.AbstractComponent.AbstractTask;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Created by a {@link LoaderTask}, handles unloading messages from a specific channel, 
 * and submits {@link SenderTask}s for sending those messages to targeted clients.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code broker != null}
 * invariant	{@code clientsInfos != null}
 * invariant	{@code chanInfos != null}
 * invariant	{@code channel != null && !channel.isEmpty()}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class UnloaderTask extends AbstractTask {
	
	/** The broker component having the task. */
	private Broker broker;
	
	/** Mapping : client inbound port (URI) -> its informations. From the {@link Broker}. */
	private Map<String, ClientBrokerInfos> clientsInfos; 
	
	/** A channel informations, from where messages will be unloaded. */
	private ChannelBrokerInfos chanInfos;
	
	/** The name of the channel having {@code chanInfos}. */
	private String channel;
	
	/**
	 * Broker publication task handling unloading messages from a specific channel.
	 * @param broker			The broker component having the task.
	 * @param clientsInfos		{@code broker} mapping : client inbound port (URI) -> its informations.
	 * @param chanInfos			A channel informations, from where messages will be unloaded.
	 * @param channel			The name of the channel having {@code chanInfos}.
	 */
	public UnloaderTask(Broker broker, Map<String, ClientBrokerInfos> clientsInfos, ChannelBrokerInfos chanInfos, String channel) {
		// Precondition checks
		assert(broker != null) : new PreconditionException("broker cannot be null");
		assert(clientsInfos != null) : new PreconditionException("clientsInfos cannot be null");
		assert(chanInfos != null) : new PreconditionException("chanInfos cannot be null");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		this.broker = broker;
		this.clientsInfos = clientsInfos;
		this.chanInfos = chanInfos;
		this.channel = channel;
	}

	@Override
	public void run() {
		try {
			// Polls remaining messages from the channel (one after one)
			MessageI msg = chanInfos.getMessage();
			while (msg != null) {
				
				// Gets the channel clients targeted by this message AT THIS MOMENT
				List<String> targetClientsPortURI = chanInfos.targetSubscribers(msg);
				
				// Sends them the message
				for (String targetPortURI : targetClientsPortURI) {
					
					MessageI[] messagesToSend = new MessageI[1]; 
					messagesToSend[0] = msg;
					
					RegistrationClass rc = clientsInfos.get(targetPortURI).getRegistrationClass();
					if (rc == RegistrationClass.FREE) {
						broker.runTask(Broker.FREE_SENDERS_POOL_URI, new SenderTask(broker, clientsInfos, channel, targetPortURI, messagesToSend));
					} else if (rc == RegistrationClass.STANDARD) {
						broker.runTask(Broker.STANDARD_REQUEST_HANDLER_URI, new SenderTask(broker, clientsInfos, channel, targetPortURI, messagesToSend));
					} else {
						broker.runTask(Broker.PREM_SENDERS_POOL_URI, new SenderTask(broker, clientsInfos, channel, targetPortURI, messagesToSend));
					}
					
				}
				
				// If the channel has been destroyed (with destroyChannel for instance) : throws an UnknownChannelException
				if (!broker.channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist anymore (destroyed).");
				msg = chanInfos.getMessage();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
