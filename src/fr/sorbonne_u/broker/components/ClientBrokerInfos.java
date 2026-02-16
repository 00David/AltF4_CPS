package fr.sorbonne_u.broker.components;

import fr.sorbonne_u.broker.ports.BrokerReceivingOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Classe gardant toutes les infos d'un {@link Client} enregistré sur le {@link Broker}.
 * 
 * <p>
 * Les infos maintenues sont :
 * <li> - L'URI du port client.
 * <li> - La {@link RegistrationClass} du client.
 * <li> - Le {@link BrokerReceivingOutboundPort}, port sortant vers ce lient.
 * </p>
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientBrokerInfos {

	private final String clientPortURI;
	private RegistrationClass rc;
	private final BrokerReceivingOutboundPort clientOutboundPort;
	
	public ClientBrokerInfos(String clientPortURI, RegistrationClass rc, BrokerReceivingOutboundPort clientOutboundPort) {
		assert(clientPortURI != null && !clientPortURI.isEmpty());
		assert(rc != null);
		assert(clientOutboundPort != null);
		
		this.clientPortURI = clientPortURI;
		this.rc = rc;
		this.clientOutboundPort = clientOutboundPort;
	}
	
	public RegistrationClass getRegistrationClass() {
		return rc;
	}
	
	public void setRegistrationClass(RegistrationClass rc) {
		this.rc = rc;
	}
	
	public BrokerReceivingOutboundPort getOutboundPort() {
		return clientOutboundPort;
	}
	
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientBrokerInfos that = (ClientBrokerInfos) o;
        return clientPortURI.equals(that.clientPortURI);
    }

    @Override
    public int hashCode() { return clientPortURI.hashCode(); }
}
