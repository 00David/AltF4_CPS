package fr.sorbonne_u.clients.ports;

import java.awt.Component;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.examples.basic_cs.components.URIProvider;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * @author Kevin TANG
 * @author David VADIMON
 * 
 **/


/**
 * La classe <code>ClientReceivingInboundPort</code> implement le port entrant pour la reception des messages.
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
		this.getOwner().runTask(
				new AbstractComponent.AbstractTask() {
					
					@Override
					public void run() {
						try {
							((ReceivingCI)this.getTaskOwner()).receive(channel, message);
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					}
				}
				);
		
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		this.getOwner().runTask(
				new AbstractComponent.AbstractTask() {
					
					@Override
					public void run() {
						try {
							((ReceivingCI)this.getTaskOwner()).receive(channel, messages);
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					}
				}
				);
		
	}
}
