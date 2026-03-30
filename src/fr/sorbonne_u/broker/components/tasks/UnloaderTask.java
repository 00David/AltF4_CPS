package fr.sorbonne_u.broker.components.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.components.infos.ChannelBrokerInfos;
import fr.sorbonne_u.broker.components.infos.ClientBrokerInfos;
import fr.sorbonne_u.components.AbstractComponent.AbstractTask;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
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
 * invariant	{@code MAX_NB_MSG_FREE_CHANNEL >= 1}
 * invariant	{@code MAX_NB_MSG_STD_CHANNEL >= MAX_NB_MSG_FREE_CHANNEL}
 * invariant	{@code MAX_NB_MSG_PREM_CHANNEL >= MAX_NB_MSG_STD_CHANNEL}
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
	
	/** The maximum number of messages to unload from {@code channel}. */
	private int nbMaxUnloadMessages;
	
	/** The maximum number of unloaded messages when the {@code channel} creator is the {@code broker}, or has unregistered from the system. */
	public static final int NB_MAX_UNLOAD_MSG_NORMAL_CHANNEL = 1;
	
	/** The maximum number of unloaded messages when the {@code channel} creator is STANDARD. */
	public static final int NB_MAX_UNLOAD_MSG_STD_CHANNEL = 5;
	
	/** The maximum number of unloaded messages when the {@code channel} creator is PREMIUM. */
	public static final int NB_MAX_UNLOAD_MSG_PREM_CHANNEL = 10;
	
	/**
	 * Broker publication task handling unloading messages from a specific channel.
	 * 
	 * <pre>
	 * pre	{@code broker != null}
	 * pre	{@code clientsInfos != null}
	 * pre	{@code chanInfos != null}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post class invariants satisfied
	 * </pre>
	 * 
	 * @param broker				The broker component having the task.
	 * @param clientsInfos			{@code broker} mapping : client inbound port (URI) -> its informations.
	 * @param chanInfos				A channel informations, from where messages will be unloaded.
	 * @param channel				The name of the channel having {@code chanInfos}.
	 * @param nbMaxUnloadMessages 	The maximum number of messages to unload from {@code channel}.
	 */
	public UnloaderTask(Broker broker, Map<String, ClientBrokerInfos> clientsInfos, ChannelBrokerInfos chanInfos, 
			String channel, int nbMaxUnloadMessages) {
		// Precondition checks
		assert(broker != null) : new PreconditionException("broker cannot be null");
		assert(clientsInfos != null) : new PreconditionException("clientsInfos cannot be null");
		assert(chanInfos != null) : new PreconditionException("chanInfos cannot be null");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		this.broker = broker;
		this.clientsInfos = clientsInfos;
		this.chanInfos = chanInfos;
		this.channel = channel;
		this.nbMaxUnloadMessages = nbMaxUnloadMessages;
	}

	@Override
	public void run() {
		// Mapping : client target port URI -> messages to send to this target
		Map<String, List<MessageI>> targetsMessages = null;
		
		try {
			//broker.logMessage("UNLOADER "+Thread.currentThread().getId()+" starts unloading from'"+channel+"'") ;

			boolean isUnloading = true;
			
			// Unloads messages from the channel. Stops when no messages.
			while (isUnloading) {
				
				// Map reseted for each unloading
				targetsMessages = new HashMap<>();
				
				int nbMessages = 0;
				// Unloads messages until it reaches the 'nbMaxUnloadMessages' or there is no messages (isUnloading == false)
				while (nbMessages < nbMaxUnloadMessages && isUnloading) {
					
					// If the channel has been destroyed (with destroyChannelService for instance) : throws an UnknownChannelException
					if (!broker.channelExistService(channel)) throw new UnknownChannelException("Channel "+channel+" doesn't exist anymore (destroyed).");
					
					// Unloads a message
					MessageI msg = chanInfos.unloadMessage();
					if (msg == null) {
						isUnloading = false;
						break;
					}
					nbMessages = nbMessages+1;
					
					// Gets the channel clients targeted by this message AT THIS MOMENT + updates the map
					List<String> targetClientsPortURI = chanInfos.targetSubscribers(msg);
					for (String target : targetClientsPortURI) {
						if (!targetsMessages.containsKey(target)) targetsMessages.put(target, new ArrayList<>());
						targetsMessages.get(target).add(msg);
					}
				}
				
				// For each target : creates a send task, sending to the target its list of targeted messages (converted to an array)
				createSendingTasks(targetsMessages);
			}

			//broker.logMessage("UNLOADER "+Thread.currentThread().getId()+" ended task") ;
		} catch (UnknownChannelException e) {
			System.err.println("Thread "+Thread.currentThread().getId()+": Unloader UnknownChannelException : "+e.getMessage());
			
			// If an unloading is interrupted, with for instance a channel destruction,
			// it will still send unloaded messages currently in its map
			createSendingTasks(targetsMessages);
		} finally {
			// Decreases the number of threads currently unloading the channel
			synchronized(chanInfos.nbActiveUnloadersLock) {
				chanInfos.nbActiveUnloaders--;
			}
		}
	}
	
	/**
	 * For each target : creates a send task, sending to the target its list of targeted messages (converted to an array).
	 * 
	 * <pre>
	 * pre	{@code targetsMessages != null}
	 * post {@code true} // no postcondition
	 * </pre>
	 * 
	 * @param targetsMessages	Mapping : client target port URI -> messages to send to this target.
	 */
	private void createSendingTasks(Map<String, List<MessageI>> targetsMessages) {
		// Precondition check
		assert(targetsMessages != null) : new PreconditionException("targetsMessages cannot be null");
		
		for (Map.Entry<String, List<MessageI>> e : targetsMessages.entrySet()) {
			String targetPortURI = e.getKey();
			List<MessageI> listMessagesToSend = e.getValue();
			
			int l = listMessagesToSend.size();
			MessageI[] messagesToSend = new MessageI[l];
			for(int i=0; i<l; i++) messagesToSend[i] = listMessagesToSend.get(i);
			
			broker.runTask(Broker.SENDERS_POOL_URI, new SenderTask(broker, clientsInfos, channel, targetPortURI, messagesToSend));
		}
	}
}
