package fr.sorbonne_u.meteo_station.components;

import fr.sorbonne_u.clients.components.Client;

/**
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoStation extends Client {

	// --------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------

	protected MeteoStation(String registrationOutboundPortURI, String publishingOutboundPortURI,
			String receivingInboundPortURI) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);

		this.getTracer().setTitle("Station Meteo");
		this.getTracer().setRelativePosition(3, 1);
	}

}
