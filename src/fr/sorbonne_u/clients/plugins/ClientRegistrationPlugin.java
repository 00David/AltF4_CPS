package fr.sorbonne_u.clients.plugins;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.connectors.ClientRegistrationServiceConnector;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientRegistrationI;

public class ClientRegistrationPlugin extends AbstractPlugin implements ClientRegistrationI {

	private static final long serialVersionUID = 612748100445996427L;
	
	protected final String receivingInboundPortURI;
	
	protected final String registrationOutboundPortURI;
	protected ClientRegistrationOutboundPort registrationOutboundPort;
	
	public ClientRegistrationPlugin(String receivingInboundPortURI, String registrationOutboundPortURI){
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationOutboundPortURI = registrationOutboundPortURI;
	}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addRequiredInterface(RegistrationCI.class);
		this.registrationOutboundPort = new ClientRegistrationOutboundPort(registrationOutboundPortURI, this.getOwner());
		this.registrationOutboundPort.publishPort();
	}
	
	@Override
	public void	initialise() throws Exception {
		this.getOwner().doPortConnection(this.registrationOutboundPort.getPortURI(),
				  Broker.registrationPortURI(),
				  ClientRegistrationServiceConnector.class.getCanonicalName());
		super.initialise();
	}
	
	@Override
	public void	finalise() throws Exception {
		this.getOwner().doPortDisconnection(this.registrationOutboundPort.getPortURI());
	}
	
	@Override
	public void uninstall() throws Exception {
		this.registrationOutboundPort.unpublishPort();
		this.removeRequiredInterface(RegistrationCI.class);
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------

	@Override
	public boolean registered() {
		try {
			return registrationOutboundPort.registered(receivingInboundPortURI);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean registered(RegistrationClass rc) throws UnknownClientException {
		try {
			return registrationOutboundPort.registered(receivingInboundPortURI, rc);
		} catch (UnknownClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void register(RegistrationClass rc) throws AlreadyRegisteredException {
		try {
			registrationOutboundPort.register(receivingInboundPortURI, rc);
		} catch (AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void modifyServiceClass(RegistrationClass rc) throws UnknownClientException, AlreadyRegisteredException {
		try {
			registrationOutboundPort.modifyServiceClass(receivingInboundPortURI, rc);
		} catch (UnknownClientException | AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void unregister() throws UnknownClientException {
		try {
			registrationOutboundPort.unregister(receivingInboundPortURI);
		} catch (UnknownClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// -------------------------------------------------------------------------
	// Port getter
	// -------------------------------------------------------------------------
	
	public ClientRegistrationOutboundPort getRegistrationOutboundPort() {
		return registrationOutboundPort;
	}
	
}
