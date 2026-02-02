package fr.sorbonne_u.clients.components;

import java.nio.channels.AlreadyConnectedException;

import fr.sorbonne_u.clients.ports.ClientPublishingOutboundPort;
import fr.sorbonne_u.clients.ports.ClientReceivingInboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * @author Kevin TANG
 * 
 **/

@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})


public class Client extends AbstractComponent {
	
	protected ClientRegistrationOutboundPort registrationOutboundPort;
	protected ClientPublishingOutboundPort publishingOutboundPort;
	protected ClientReceivingInboundPort receivingInboundPort;

	protected Client(String uri,
			String registrationOutboundPortURI,String publishingOutboundPortURI,String receivingInboundPortURI) throws Exception {
		super(uri,0,1);
		
		//initializing outbound and inbound ports
		this.registrationOutboundPort = new ClientRegistrationOutboundPort(registrationOutboundPortURI, this);
		this.publishingOutboundPort = new ClientPublishingOutboundPort(publishingOutboundPortURI, this);
		this.receivingInboundPort = new ClientReceivingInboundPort(receivingInboundPortURI, this);
		
		//publish outbound ports
		this.registrationOutboundPort.localPublishPort();
		this.receivingInboundPort.localPublishPort();
		
		if (AbstractCVM.isDistributed) {
			this.getLogger().setDirectory(System.getProperty("user.dir")) ;
		} else {
			this.getLogger().setDirectory(System.getProperty("user.home")) ;
		}
		this.getTracer().setTitle("consumer") ;
		this.getTracer().setRelativePosition(1, 1) ;

		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
	}
	
	
	
	
	
	
	
	//component life cycle
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting client component");
		super.start() ;
	}
		
	
	@Override
	public void execute() throws Exception{
		super.execute();
		this.logMessage("executing client component");
		
		if(!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			String brokerURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
		}else {
			String msg = "Client already registred.";
			throw new AlreadyRegisteredException(msg);
		}
		
	}
	
	
	@Override
	public void finalise() throws Exception{
		this.doPortDisconnection(registrationOutboundPort.getPortURI());
		this.doPortDisconnection(publishingOutboundPort.getPortURI());
		this.doPortDisconnection(receivingInboundPort.getPortURI());
		
		super.finalise();
	}
	
	
	
	

}
