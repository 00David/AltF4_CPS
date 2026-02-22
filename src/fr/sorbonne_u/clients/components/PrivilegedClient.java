package fr.sorbonne_u.clients.components;

import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

@RequiredInterfaces(required = { RegistrationCI.class, PrivilegedClientCI.class })
@OfferedInterfaces(offered = { ReceivingCI.class })
public abstract class PrivilegedClient extends Client {

	protected PrivilegedClient(String registrationOutboundPortURI, String publishingOutboundPortURI,
			String receivingInboundPortURI) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
	}

}
