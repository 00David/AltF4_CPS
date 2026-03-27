package fr.sorbonne_u.broker.components.tasks;

import java.util.Map;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.broker.components.infos.ClientBrokerInfos;
import fr.sorbonne_u.components.AbstractComponent.AbstractTask;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Created by a {@link UnloaderTask}, handles messages sending to a specific targeted client.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code broker != null}
 * invariant	{@code clientsInfos != null}
 * invariant	{@code channel != null && !channel.isEmpty()}
 * invariant	{@code targetPortURI != null && !targetPortURI.isEmpty()}
 * invariant	{@code messagesToSend != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class SenderTask extends AbstractTask {
	
	/** The broker component having the task. */
	private Broker broker;
	
	/** Mapping : client inbound port (URI) -> its informations. From the {@link Broker}. */
	private Map<String, ClientBrokerInfos> clientsInfos; 
	
	/** The channel from where {@code messagesToSend} has been unloaded. */
	private String channel;
	
	/** The targeted client port URI, where {@code messagesToSend} must be sent. */
	private String targetPortURI;
	
	/** The messages to send to the {@code targetPortURI}. */
	private MessageI[] messagesToSend;
	
	/**
	 * Broker publication task handling unloading messages from a specific channel.
	 * 
	 * <pre>
	 * pre	{@code broker != null}
	 * pre	{@code clientsInfos != null}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code targetPortURI != null}
	 * pre	{@code messagesToSend != null}
	 * post class invariants satisfied
	 * </pre>
	 * 
	 * @param broker			The broker component having the task.
	 * @param clientsInfos		{@code broker} mapping : client inbound port (URI) -> its informations.
	 * @param channel			The channel from where {@code messagesToSend} has been unloaded.
	 * @param targetPortURI		The targeted client port URI, where {@code messagesToSend} must be sent.
	 * @param messagesToSend	The messages to send to the {@code targetPortURI}.
	 */
	public SenderTask(Broker broker, Map<String, ClientBrokerInfos> clientsInfos, 
			String channel, String targetPortURI, MessageI[] messagesToSend) {
		// Precondition checks
		assert(broker != null) : new PreconditionException("broker cannot be null");
		assert(clientsInfos != null) : new PreconditionException("clientsInfos cannot be null");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(targetPortURI != null && !targetPortURI.isEmpty()) : new PreconditionException("targetPortURI cannot be null or empty");
		assert(messagesToSend != null) : new PreconditionException("messagesToSend cannot be null");
		
		this.broker = broker;
		this.clientsInfos = clientsInfos;
		this.channel = channel;
		this.targetPortURI = targetPortURI;
		this.messagesToSend = messagesToSend;
	}

	@Override
	public void run() {
		try {
			//broker.logMessage("SENDER "+Thread.currentThread().getId()+" starts sending from'"+channel+"' to '"+targetPortURI+"'") ;
			
			// Takes the client lock
			ClientBrokerInfos client = clientsInfos.get(targetPortURI);
			synchronized(client) {
				// Checks that the targeted client is still registered. If it is okay, sends messages.
				if (broker.registeredService(targetPortURI)) {
					broker.logMessage("Thread "+Thread.currentThread().getId()+": Broker sending "+messagesToSend.length+" message(s) to '" + targetPortURI + "'") ;
					client.getOutboundPort().receive(channel, messagesToSend);
				}
			}

			//broker.logMessage("SENDER "+Thread.currentThread().getId()+" ended task") ;
		} catch (Exception e) {
			System.err.println("Thread "+Thread.currentThread().getId()+": Sender error : "+e.getMessage());
		}
	}
}
