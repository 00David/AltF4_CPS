package fr.sorbonne_u.broker.components;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Classe gardant toutes les infos d'un channel existant sur le {@link Broker}.
 * 
 * <p>
 * Chaque instance est identifiée uniquement par son attribut {@code channel}.
 * </p>
 * 
 * <p>
 * Les infos maintenues sont :
 * <li> - Le nom du channel.
 * <li> - Un ensemble de paires : (port d'un client abonné + filtre sur le channel)
 * <li> - La {@link RegistrationClass} minimale d'enregistrement acceptée pour ce channel.
 * <li> - Le port client ayant créé le channel, ou null si c'est le Broker qui l'a créé au démarrage.
 * <li> - Une liste de {@link MessageI} du channel.
 * </p>
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ChannelBrokerInfos {
	
	/**
	 * Paire : port d'un client abonné + filtre sur le channel.
	 * 
	 * <p>
	 * Chaque instance est identifiée uniquement par son attribut {@code subscriberPortURI}.
	 * </p>
	 * 
	 * @author Kevin Tang
	 * @author David Vadimon
	 */
	private static final class SubsciberFilter {
	    private final String subscriberPortURI;
	    private final MessageFilterI filter;

	    /**
	     * Crée une paire : port d'un client abonné + filtre sur le channel.
	     * 
		 * <p><strong>Contract</strong></p>
		 * <pre>
		 * pre	{@code clientPortURI != null && !clientPortURI.isEmpty()}
		 * pre	{@code filter != null}
		 * post	{@code true}	// no postcondition.
		 * </pre>
	     * 
	     * @param clientPortURI 	l'URI du port entrant du client, offrant l'interface de composant {@code ReceivingCI}.
	     * @param filter			le filtre du client sur le channel.
	     */
	    private SubsciberFilter(String clientPortURI, MessageFilterI filter) {
	    	assert(clientPortURI != null && !clientPortURI.isEmpty());
	    	assert(filter != null);
	    	
	        this.subscriberPortURI = clientPortURI;
	        this.filter = filter;
	    }
	    
		// Un SubsciberFilter n'est identifié que par l'URI du port du client abonné concerné
		// => redéfinition de equals et hashCode
	    
	    @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (o == null || getClass() != o.getClass()) return false;

	        SubsciberFilter that = (SubsciberFilter) o;
	        return subscriberPortURI.equals(that.subscriberPortURI);
	    }

	    @Override
	    public int hashCode() { return subscriberPortURI.hashCode(); }
	}
	
	private final String channel;
	private final Set<SubsciberFilter> subscribers;
	private RegistrationClass minRC;
	private final String clientCreator;
	private final List<MessageI> messages;

	/**
	 * Construction des infos d'un channel enregistré sur le {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code minRC != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel			le nom du channel.
	 * @param minRC				la {@link RegistrationClass} minimale acceptée par ce channel.
	 * @param clientCreator		l'URI du port du client ayant créé le {@code channel}, ou {@code null} si c'est un canal créé par le {@link Broker}.
	 */
	public ChannelBrokerInfos(String channel, RegistrationClass minRC, String clientCreator) {
		// Vérification des préconditions
		assert(channel != null && !channel.isEmpty());
		assert(minRC != null);
		
		this.channel = channel;
		this.subscribers = new HashSet<>();
		this.minRC = minRC;
		this.clientCreator = clientCreator;
		this.messages = new ArrayList<>();
	}
	
	// SUBSCRIBERS (+ THEIR FILTER) MANAGEMENT
	
	/**
	 * Ajoute ou remplace un {@code clientPortURI} abonné au channel, via un {@link SubsciberFilter}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code clientPortURI != null && !clientPortURI.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code isSubscribed(clientPortURI)}
	 * </pre>
	 * 
	 * @param clientPortURI 		l'URI du port entrant du client, offrant l'interface de composant {@code ReceivingCI}.
	 * @param filter				le filtre sur le channel
	 */
	public void addOrReplaceSubscriber(String clientPortURI, MessageFilterI filter) {
		// Vérification des préconditions
		assert(clientPortURI != null && !clientPortURI.isEmpty());
		assert(filter != null);
		
		subscribers.add(new SubsciberFilter(clientPortURI, filter));
		
		// Vérification de la postcondition
		assert(isSubscribed(clientPortURI));
	}
	
	/**
	 * Vérifie si un {@code clientPortURI} donné est abonné au channel.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code clientPortURI != null && !clientPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param clientPortURI		l'URI du port entrant du client, offrant l'interface de composant {@code ReceivingCI}.
	 * @return 					true si {@code clientPortURI} est abonné, false sinon.
	 */
	public boolean isSubscribed(String clientPortURI) {
		// Vérification des préconditions
		assert(clientPortURI != null && !clientPortURI.isEmpty());
		
		return subscribers.contains(new SubsciberFilter(clientPortURI, null));
	}
	
	/**
	 * Supprime un {@code clientPortURI} abonné au channel.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code clientPortURI != null && !clientPortURI.isEmpty()}
	 * pre	{@code isSubscribed(clientPortURI)}
	 * post	{@code !isSubscribed(clientPortURI)}
	 * </pre>
	 * 
	 * @param clientPortURI		l'URI du port entrant du client, offrant l'interface de composant {@code ReceivingCI}.
	 */
	public void removeSubscriber(String clientPortURI) {
		// Vérification des préconditions
		assert(clientPortURI != null && !clientPortURI.isEmpty());
		assert(isSubscribed(clientPortURI));
		
		subscribers.remove(new SubsciberFilter(clientPortURI, null));
		
		// Vérification de la postcondition
		assert(!isSubscribed(clientPortURI));
	}
	
	public Set<SubsciberFilter> getSubscribers() {
		return subscribers;
	}
	
	// MINIMUM REGISTRATION CLASS MANAGEMENT
	
	public RegistrationClass getMinRC() {
		return minRC;
	}
	
	public void setMinRC(RegistrationClass minRC) {
		// Vérification de la précondition
		assert(minRC != null);
		
		this.minRC = minRC;
	}
	
	// CREATOR MANAGEMENT
	
	public String getCreator() {
		return clientCreator;
	}
	
	// MESSAGES MANAGEMENT
	
	public void addMessage(MessageI msg) {
		// Vérification de la précondition
		assert(msg != null);
		
		messages.add(msg);
	}
	
	public void addMessages(List<MessageI> msgs) {
		// Vérification des préconditions
		assert(msgs != null && msgs.size() > 0);
		
		messages.addAll(msgs);
	}
	
	public List<MessageI> getMessages() {
		return messages;
	}
	
	public List<String> targetSubscribers(MessageI msg){
		// Vérification des préconditions
		assert(msg != null);
		assert(messages.contains(msg));
		
		List<String> targetSubs = new ArrayList<>();
		
		for (SubsciberFilter sub : subscribers) {
			if (sub.filter.match(msg)) targetSubs.add(sub.subscriberPortURI);
		}
		
		return targetSubs;
	}
	
	
	// Un ChannelBrokerInfos n'est identifié que par le nom du channel concerné
	// => redéfinition de equals et hashCode
	
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChannelBrokerInfos that = (ChannelBrokerInfos) o;
        return channel.equals(that.channel);
    }

    @Override
    public int hashCode() { return channel.hashCode(); }
}
