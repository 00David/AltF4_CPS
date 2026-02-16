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
	
	// Paire port d'un client abonné + filtre sur le channel
	// La paire n'est identifiée QUE par le port du client
	private static final class ClientChannelFilter {
	    private final String clientPortURI;
	    private final MessageFilterI filter;

	    private ClientChannelFilter(String clientPortURI,
	                                 MessageFilterI filter) {
	        this.clientPortURI = clientPortURI;
	        this.filter = filter;
	    }
	    
	    @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (o == null || getClass() != o.getClass()) return false;

	        ClientChannelFilter that = (ClientChannelFilter) o;
	        return clientPortURI.equals(that.clientPortURI);
	    }

	    @Override
	    public int hashCode() { return clientPortURI.hashCode(); }
	}
	
	private final String channel;
	private final Set<ClientChannelFilter> subscribers;
	private RegistrationClass minRC;
	private final String clientCreator;
	private final List<MessageI> messages;

	public ChannelBrokerInfos(String channel, RegistrationClass minRC, String clientCreator) {
		this.channel = channel;
		this.subscribers = new HashSet<>();
		this.minRC = minRC;
		this.clientCreator = clientCreator;
		this.messages = new ArrayList<>();
	}
	
	public void addOrReplaceClientChannelFilter(String clientPortURI, MessageFilterI filter) {
		subscribers.add(new ClientChannelFilter(clientPortURI, filter));
	}
	
	public boolean isSubscribed(String clientPortURI) {
		return subscribers.contains(new ClientChannelFilter(clientPortURI, null));
	}
	
	public RegistrationClass getMinRC() {
		return minRC;
	}
	
	public void setMinRC(RegistrationClass minRC) {
		this.minRC = minRC;
	}
	
	public String getCreator() {
		return clientCreator;
	}
	
	public void addMessage(MessageI msg) {
		messages.add(msg);
	}
	
	public void addMessages(List<MessageI> msgs) {
		messages.addAll(msgs);
	}
	
	public List<MessageI> getMessages() {
		return messages;
	}
	
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
