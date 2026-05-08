package fr.sorbonne_u.broker.component.tasks;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.broker.component.ClientBrokerInfos;
import fr.sorbonne_u.broker.component.VisibilitySenderTask;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Created by a {@link UnloaderTask}, handles messages sending to a specific targeted client.
 * 
 * <p>
 * This class derives from {@link VisibilitySenderTask} in order to have an access to the targeted client sending method,
 * not visible from outside {@link Broker}'s package. 
 * </p>
 * 
 * <p>
 * An {@link Exception} can be thrown while sending messages, 
 * in that case an error log appears both in the Broker log window and on the console.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code broker != null}
 * invariant	{@code channel != null && !channel.isEmpty()}
 * invariant	{@code target != null}
 * invariant	{@code targetPortURI != null && !targetPortURI.isEmpty()}
 * invariant	{@code messagesToSend != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class SenderTask extends VisibilitySenderTask {
	
	/** The broker component having the task. */
	private final Broker broker;
	
	/** The channel from where {@code messagesToSend} has been unloaded. */
	private final String channel;
	
	/** The targeted client. Sending is delegated via this object. */
	private final ClientBrokerInfos target;
	
	/** The targeted client port URI, where {@code messagesToSend} must be sent. */
	private final String targetPortURI;
	
	/** The messages to send to the {@code targetPortURI}. */
	private final MessageI[] messagesToSend;
	
	/**
	 * Broker publication task handling unloading messages from a specific channel.
	 * 
	 * <pre>
	 * pre	{@code broker != null}
	 * pre	{@code channel != null}
	 * pre	{@code target != null}
	 * pre	{@code targetPortURI != null && !targetPortURI.isEmpty()}
	 * pre	{@code messagesToSend != null}
	 * post class invariants satisfied
	 * </pre>
	 * 
	 * @param broker			The broker component having the task.
	 * @param channel			The channel from where {@code messagesToSend} has been unloaded.
	 * @param target			The targeted client. Sending is delegated via this object.
	 * @param targetPortURI		The targeted client port URI, where {@code messagesToSend} must be sent.
	 * @param messagesToSend	The messages to send to the {@code targetPortURI}.
	 */
	public SenderTask(Broker broker, String channel,
			ClientBrokerInfos target, String targetPortURI, MessageI[] messagesToSend) {
		// Precondition checks
		assert(broker != null) : 
			new PreconditionException("broker cannot be null");
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(target != null) : 
			new PreconditionException("target cannot be null");
		assert(targetPortURI != null && !targetPortURI.isEmpty()) : 
			new PreconditionException("targetPortURI cannot be null or empty");
		assert(messagesToSend != null) : 
			new PreconditionException("messagesToSend cannot be null");
		
		this.broker = broker;
		this.channel = channel;
		this.target = target;
		this.targetPortURI = targetPortURI;
		this.messagesToSend = messagesToSend;
	}

	@Override
	public void run() {
		try {
			//broker.logMessage("SENDER "+Thread.currentThread().getId()+" starts sending from'"+channel+"' to '"+targetPortURI+"'") ;
			
			// Checks that the targeted client is still registered AT THE MOMENT
			// If it is okay, send messages if the client is on the same JVM.
			if (broker.registeredService(targetPortURI)) {
				boolean haveBeenSent = sendMessages(target, channel, messagesToSend);
				if (haveBeenSent) {
					broker.logMessage("Thread "+Thread.currentThread().getId()+": Broker sending "+messagesToSend.length+" message(s) to '" + targetPortURI + "'") ;
				}
			}

			//broker.logMessage("SENDER "+Thread.currentThread().getId()+" ended task") ;
		} catch (Exception e) {
			System.err.println("Broker"+broker.id+", Thread "+Thread.currentThread().getId()+": Sender Exception : "+e.getMessage());
			broker.logMessage("[ERROR] Thread "+Thread.currentThread().getId()+": Sender Exception : "+e.getMessage());
		}
	}
}
