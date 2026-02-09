package fr.sorbonne_u.windturbine.components;

import fr.sorbonne_u.clients.components.Client;

public class WindTurbine extends Client {

	protected WindTurbine(String registrationOutboundPortURI, String publishingOutboundPortURI,
			String receivingInboundPortURI) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		
	}

}
