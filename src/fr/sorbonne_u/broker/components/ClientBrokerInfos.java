package fr.sorbonne_u.broker.components;

import fr.sorbonne_u.broker.ports.BrokerReceivingOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Classe gardant toutes les infos d'un {@link Client} enregistré sur le {@link Broker}.
 * 
 * <p>
 * Chaque instance est identifiée uniquement par son attribut {@code clientPortURI}.
 * </p>
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
	
	// Setup par le Broker, dans son constructeur
	public static int nbMaxChannelsStd = -1;
	public static int nbMaxChannelsPrem = -1;

	private final String clientPortURI;
	private RegistrationClass rc;
	private final BrokerReceivingOutboundPort clientOutboundPort;
	private int nbChannelsRemaining;
	
	/**
	 * Construction des infos d'un {@link Client} enregistré sur le {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code clientPortURI != null && !clientPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * pre	{@code clientOutboundPort != null}
	 * pre	{@code nbMaxChannelsStd != -1}
	 * pre	{@code nbMaxChannelsPrem != -1}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param clientPortURI			l'URI du port entrant du client, offrant l'interface de composant {@code ReceivingCI}.
	 * @param rc					la classe d'enregistrement du client.
	 * @param clientOutboundPort	le port sortant du {@link Broker} requérant l'interface {@code ReceivingCI}, associé au client.
	 */
	public ClientBrokerInfos(String clientPortURI, RegistrationClass rc, BrokerReceivingOutboundPort clientOutboundPort) {
		assert(clientPortURI != null && !clientPortURI.isEmpty());
		assert(rc != null);
		assert(clientOutboundPort != null);
		assert(nbMaxChannelsStd != -1);
		assert(nbMaxChannelsPrem != -1);
		
		this.clientPortURI = clientPortURI;
		this.rc = rc;
		this.clientOutboundPort = clientOutboundPort;
		
		switch(rc) {
		case FREE:
			this.nbChannelsRemaining = 0;
		case STANDARD:
			this.nbChannelsRemaining = nbMaxChannelsStd;
		default:
			this.nbChannelsRemaining = nbMaxChannelsPrem;
		}
	}
	
	// REGISTRATION CLASS MANAGEMENT
	
	/**
	 * Renvoit la {@link RegistrationClass} du client.
	 * @return la {@link RegistrationClass} du client.
	 */
	public RegistrationClass getRegistrationClass() {
		return rc;
	}
	
	/**
	 * Remplace la {@link RegistrationClass} du client.
	 * @param rc la nouvelle {@link RegistrationClass} du client.
	 */
	public void setRegistrationClass(RegistrationClass rc) {
		this.rc = rc;
	}
	
	// OUTBOUND PORT MANAGEMENT
	
	/**
	 * Renvoit le {@link BrokerReceivingOutboundPort} du client.
	 * @return le {@link BrokerReceivingOutboundPort} du client.
	 */
	public BrokerReceivingOutboundPort getOutboundPort() {
		return clientOutboundPort;
	}
	
	// NUMBER OF REMAINING CHANNELS MANAGEMENT
	
	/**
	 * Indique si le client peut créer au moins un channel.
	 * @return true si au moins 1 canal peut être créé par le client, false sinon.
	 */
	public boolean canCreateChannel() {
		return nbChannelsRemaining > 0;
	}
	
	/**
	 * Décrémente le nombre de canaux pouvant être créés par le client.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code this.nbChannelsRemaining > 0}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 */
	public void decreaseRemainingChannels() {
		// Vérification de la précondition
		assert(nbChannelsRemaining > 0);
		
		nbChannelsRemaining--;
	}
	
	/**
	 * Incrémente le nombre de canaux pouvant être créés par le client.
	 */
	public void increaseRemainingChannels() {
		nbChannelsRemaining++;
	}
	
	// Un ClientBrokerInfos n'est identifié que par l'URI du port du client
	// => redéfinition de equals et hashCode
	
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
