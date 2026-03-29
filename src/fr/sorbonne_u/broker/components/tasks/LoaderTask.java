package fr.sorbonne_u.broker.components.tasks;

import java.util.ArrayList;
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
 * Created by the {@link Broker}, handles loading (copies of) messages into a specific channel, 
 * and submits an {@link UnloaderTask} for unloading those messages.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code broker != null}
 * invariant	{@code clientsInfos != null}
 * invariant	{@code channelsInfos != null}
 * invariant	{@code channel != null && !channel.isEmpty()}
 * invariant	{@code messagesToLoad != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class LoaderTask extends AbstractTask {
	
	/** The broker component having the task. */
	private Broker broker;
	
	/** Mapping : client inbound port (URI) -> its informations. From the {@link Broker}. */
	private Map<String, ClientBrokerInfos> clientsInfos; 
	
	/** Mapping : channel name -> its informations. From the {@link Broker}. */
	private Map<String, ChannelBrokerInfos> channelsInfos;
	
	/** The name of the channel where messages must be loaded. */
	private String channel;
	
	/** The messages to load into the {@code channel} */
	private ArrayList<MessageI>  messagesToLoad;
	
	/**
	 * Broker publication task handling loading received messages into a specific channel.
	 * 
	 * <pre>
	 * pre	{@code broker != null}
	 * pre	{@code clientsInfos != null}
	 * pre	{@code channelsInfos != null}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messagesToLoad != null}
	 * post class invariants satisfied
	 * </pre>
	 * 
	 * @param broker			The broker component having the task.
	 * @param clientsInfos		{@code broker} mapping : client inbound port (URI) -> its informations.
	 * @param channelsInfos		{@code broker} mapping : channel name -> its informations.
	 * @param channel			The name of the channel where messages must be loaded.
	 * @param messagesToLoad	The messages to load into the {@code channel}.
	 */
	public LoaderTask(Broker broker, Map<String, ClientBrokerInfos> clientsInfos, Map<String, ChannelBrokerInfos> channelsInfos, 
			String channel, ArrayList<MessageI>  messagesToLoad) {
		// Precondition checks
		assert(broker != null) : new PreconditionException("broker cannot be null");
		assert(clientsInfos != null) : new PreconditionException("clientsInfos cannot be null");
		assert(channelsInfos != null) : new PreconditionException("channelsInfos cannot be null");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(messagesToLoad != null) : new PreconditionException("messagesToLoad cannot be null");
		
		this.broker = broker;
		this.clientsInfos = clientsInfos;
		this.channelsInfos = channelsInfos;
		this.channel = channel;
		this.messagesToLoad = messagesToLoad;
	}

	@Override
	public void run() {
		try {
			//broker.logMessage("LOADER "+Thread.currentThread().getId()+" starts loading into '"+channel+"'") ;
			
			ChannelBrokerInfos chanInfos = channelsInfos.get(channel);
			if (chanInfos == null) throw new UnknownChannelException("Channel "+channel+" doesn't exist anymore.");
			
			// The maximum number of messages to unload, PER UNLOADER
			int nbMaxUnloadMessages = UnloaderTask.NB_MAX_UNLOAD_MSG_FREE_CHANNEL;
			
			// If the creator is still registered on the system, and is standard or premium, then the maximum number increases
			String channelCreator = chanInfos.getCreator();
			if (channelCreator != null) {
				RegistrationClass channelCreatorRC = clientsInfos.get(channelCreator).getRegistrationClass();
				if (channelCreatorRC == RegistrationClass.STANDARD) {
					nbMaxUnloadMessages = UnloaderTask.NB_MAX_UNLOAD_MSG_STD_CHANNEL;
				} else if (channelCreatorRC == RegistrationClass.PREMIUM) {
					nbMaxUnloadMessages = UnloaderTask.NB_MAX_UNLOAD_MSG_PREM_CHANNEL;
				}
			}
			
			// Puts copies of original messages into the channel (for not spreading shared references)
			for(MessageI msg : messagesToLoad) {
				chanInfos.loadMessage(msg.copy());
			}
			
			// Creates an unload task
			broker.runTask(Broker.UNLOADERS_POOL_URI, new UnloaderTask(broker, clientsInfos, chanInfos, channel, nbMaxUnloadMessages));
			
			//broker.logMessage("LOADER "+Thread.currentThread().getId()+" ended task") ;
		} catch (UnknownChannelException e) {
			System.err.println("Thread "+Thread.currentThread().getId()+": Loader error : "+e.getMessage());
		}
	}
}
