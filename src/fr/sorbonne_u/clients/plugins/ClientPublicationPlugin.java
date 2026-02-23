package fr.sorbonne_u.clients.plugins;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.clients.ports.ClientPublishingOutboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientPublicationI;

public class ClientPublicationPlugin extends AbstractPlugin implements ClientPublicationI {
	
	private static final long serialVersionUID = 4889836074410437984L;
	
	protected final String receivingInboundPortURI;
	
	protected ClientPublishingOutboundPort publishingOutboundPort;
	protected final String publishingOutboundPortURI;
	
	protected final ClientRegistrationOutboundPort registrationOutboundPort;
	
	public ClientPublicationPlugin(String receivingInboundPortURI, String publishingOutboundPortURI,
			ClientRegistrationOutboundPort registrationOutboundPort){
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		this.registrationOutboundPort = registrationOutboundPort;
	}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addRequiredInterface(PublishingCI.class);
		this.publishingOutboundPort = new ClientPublishingOutboundPort(publishingOutboundPortURI, this.getOwner());
		this.publishingOutboundPort.publishPort();
	}
	
	@Override
	public void	initialise() throws Exception {
		this.getOwner().doPortConnection(this.publishingOutboundPort.getPortURI(),
				  Broker.registrationPortURI(),
				  ClientPublishingServiceConnector.class.getCanonicalName());
		super.initialise();
	}
	
	@Override
	public void	finalise() throws Exception {
		this.getOwner().doPortDisconnection(this.publishingOutboundPort.getPortURI());
	}
	
	@Override
	public void uninstall() throws Exception {
		this.publishingOutboundPort.unpublishPort();
		this.removeRequiredInterface(PublishingCI.class);
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------

	@Override
	public boolean channelExist(String channel) {
		try {
			return registrationOutboundPort.channelExist(channel);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean channelAuthorised(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return registrationOutboundPort.channelAuthorised(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void publish(String channel, MessageI message)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			publishingOutboundPort.publish(receivingInboundPortURI, channel, message);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void publish(String channel, ArrayList<MessageI> messages)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			publishingOutboundPort.publish(receivingInboundPortURI, channel, messages);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
