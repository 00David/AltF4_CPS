package fr.sorbonne_u.broker.component.tasks;

import java.util.ArrayList;
import java.util.Map;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.broker.component.ChannelBrokerInfos;
import fr.sorbonne_u.broker.component.ClientBrokerInfos;
import fr.sorbonne_u.components.AbstractComponent.AbstractTask;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Created by the {@link Broker}, handles loading (copies of) messages into a specific channel, 
 * and submits an {@link UnloaderTask} for unloading those messages.
 * 
 * <p>
 * An {@link UnknownChannelException} can be thrown while checking if the channel still exists before loading, 
 * in that case an error log appears both in the Broker log window and on the console.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code broker != null}
 * invariant	{@code clientsInfos != null}
 * invariant	{@code channel != null}
 * invariant	{@code channelName != null && !channelName.isEmpty()}
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
	
	/** The channel where messages must be loaded. */
	private ChannelBrokerInfos channel;
	
	/** The name of the channel where messages must be loaded. */
	private String channelName;
	
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
	 * @param channel			The channel where messages must be loaded.
	 * @param channelName		The name of the channel where messages must be loaded.
	 * @param messagesToLoad	The messages to load into the {@code channel}.
	 */
	public LoaderTask(Broker broker, Map<String, ClientBrokerInfos> clientsInfos, 
			ChannelBrokerInfos channel, String channelName, ArrayList<MessageI>  messagesToLoad) {
		// Precondition checks
		assert(broker != null) : new PreconditionException("broker cannot be null");
		assert(clientsInfos != null) : new PreconditionException("clientsInfos cannot be null");
		assert(channel != null) : new PreconditionException("channel cannot be null");
		assert(channelName != null && !channelName.isEmpty()) : new PreconditionException("channelName cannot be null or empty");
		assert(messagesToLoad != null) : new PreconditionException("messagesToLoad cannot be null");
		
		this.broker = broker;
		this.clientsInfos = clientsInfos;
		this.channel = channel;
		this.channelName = channelName;
		this.messagesToLoad = messagesToLoad;
	}

	@Override
	public void run() {
		try {
			//broker.logMessage("LOADER "+Thread.currentThread().getId()+" starts loading into '"+channel+"'") ;
			
			if (!broker.channelExistService(channelName)) {
				throw new UnknownChannelException("Channel "+channel+" doesn't exist anymore.");
			}
			
			// Loads copies of original messages into the channel (for not spreading shared references)
			for(MessageI msg : messagesToLoad) {
				channel.loadMessage(msg.copy());
			}
			
			synchronized(channel.nbActiveUnloadersLock) {
				if (channel.nbActiveUnloaders < (broker.nbUnloadersThreads/2.0)) {
					// Creates an unload task if less than half of unloading threads are already on it
					broker.runTask(Broker.UNLOADERS_POOL_URI, new UnloaderTask(broker, clientsInfos, channel, channelName));
					channel.nbActiveUnloaders++;
				}
			}
			
			//broker.logMessage("LOADER "+Thread.currentThread().getId()+" ended task") ;
		} catch (UnknownChannelException e) {
			System.err.println("Broker"+broker.id+", Thread "+Thread.currentThread().getId()+": Loader UnknownChannelException : "+e.getMessage());
			broker.logMessage("[ERROR] Thread "+Thread.currentThread().getId()+": Loader UnknownChannelException : "+e.getMessage());
		}
	}
}
