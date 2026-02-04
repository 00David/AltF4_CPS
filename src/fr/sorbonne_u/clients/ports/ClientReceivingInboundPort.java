package fr.sorbonne_u.clients.ports;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * La classe <code>ClientReceivingInboundPort</code> implement le port entrant pour la reception des messages.
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientReceivingInboundPort extends AbstractInboundPort implements ReceivingCI {
	
	private static final long serialVersionUID = 1L;

	public ClientReceivingInboundPort(String uri,ComponentI owner) throws Exception {
		super(uri,ReceivingCI.class,owner);
		assert	uri != null && owner instanceof Client ;
	}
	
	public ClientReceivingInboundPort(ComponentI owner) throws Exception {
		super(ReceivingCI.class,owner);
		assert	owner instanceof Client ;
	}

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Client)owner).receiveService(channel, message));
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Client)owner).receiveService(channel, messages));
	}
}
