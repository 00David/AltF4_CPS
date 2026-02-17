package fr.sorbonne_u.broker.components;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

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
 * <li> - Indication si le channel est accesible seulement aux client privilégiés, ou à tout le monde (channel par défaut).
 * <li> - L'expression régulière des URIs des ports clients acceptés par le channel.
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
		 * pre	{@code subscriberPortURI != null && !subscriberPortURI.isEmpty()}
		 * post	{@code true}	// no postcondition.
		 * </pre>
	     * 
	     * @param subscriberPortURI 	l'URI du port entrant du client, offrant l'interface de composant {@code ReceivingCI}.
	     * @param filter			le filtre du client sur le channel.
	     */
	    private SubsciberFilter(String subscriberPortURI, MessageFilterI filter) {
	    	assert(subscriberPortURI != null && !subscriberPortURI.isEmpty());
	    	
	        this.subscriberPortURI = subscriberPortURI;
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
	private final boolean onlyPrivileged;
	private String authorisedUsers;
	private final String clientCreator;
	private final List<MessageI> messages;

	/**
	 * Construction des infos d'un channel enregistré sur le {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code authorisedUsers != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel			le nom du channel.
	 * @param authorisedUsers	l'expression régulière des URIs des ports clients acceptés par le channel.
	 * @param clientCreator		l'URI du port du client ayant créé le {@code channel}, ou {@code null} si c'est un canal créé par le {@link Broker}.
	 * @param onlyPrivileged	true si le channel est accesible seulement aux client privilégiés, false pour tout le monde.
	 */
	public ChannelBrokerInfos(String channel, String authorisedUsers, String clientCreator, boolean onlyPrivileged) {
		// Vérification des préconditions
		assert(channel != null && !channel.isEmpty());
		assert(authorisedUsers != null);
		
		this.channel = channel;
		this.subscribers = new HashSet<>();
		this.onlyPrivileged = onlyPrivileged;
		this.authorisedUsers = authorisedUsers;
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
	
	/**
	 * Renvoit l'ensemble des URI des ports clients abonnés au channel.
	 * @return l'ensemble des URI des ports clients abonnés au channel.
	 */
	public List<String> getSubscribers() {
		List<String> subs = new ArrayList<>();
		for (SubsciberFilter sub : subscribers) {
			subs.add(sub.subscriberPortURI);
		}
		return subs;
	}
	
	// AUTHORIZED USERS MANAGEMENT
	
	/**
	 * Indique si le channel est accesible seulement aux client privilégiés, ou à tout le monde.
	 * @return true si le channel est accesible seulement aux client privilégiés, false sinon.
	 */
	public boolean onlyPrivileged() {
		return onlyPrivileged;
	}
	
	/**
	 * Modifie l'expression régulière des utilisateurs authorisés pour ce channel.
	 * 
	 * <p>
	 * Si certains utilisateurs abonnés ne correspondent pas à la nouvelle expression régulière, ils sont désabonnés.
	 * </p>
	 * 
	 * @param authorizedUsers la nouvelle expression régulière des utilisateurs authorisés pour ce channel (URI).
	 */
	public void modifyAuthorizedUsers(String authorizedUsers) {
		this.authorisedUsers = authorizedUsers;
		
		// Désabonnement des utilisateurs abonnés qui ne matchent plus la nouvelle expression régulière
		List<String> subscribers = getSubscribers();
		for (String sub : subscribers) {
			if (!sub.matches(authorizedUsers)) removeSubscriber(sub);
		}
	}
	
	/**
	 * Renvoit l'expression régulière filtrant les URIs des ports clients pouvant s'abonner.
	 * @return l'expression régulière filtrant les URIs des ports clients pouvant s'abonner.
	 */
	public String getAuthorizedUsers() {
		return authorisedUsers;
	}

	// CREATOR MANAGEMENT
	
	/**
	 * Renvoit l'URI du port client ayant créé le channel.
	 * @return l'URI du port client ayant créé le channel.
	 */
	public String getCreator() {
		return clientCreator;
	}
	
	// MESSAGES MANAGEMENT
	
	/**
	 * Ajoute un {@link MessageI} au channel.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code msg != null}
	 * post	{@code messages.contains(msg)}
	 * </pre>
	 * 
	 * @param msg le nouveau {@link MessageI} à ajouter au channel.
	 */
	public void addMessage(MessageI msg) {
		// Vérification de la précondition
		assert(msg != null);
		
		messages.add(msg);
		
		// Vérification de la postcondition
		assert(messages.contains(msg));
	}
	
	/**
	 * Ajoute l'ensemble des {@link MessageI} au channel.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code msg != null}
	 * post	{@code messages.contains(msg)}
	 * </pre>
	 * 
	 * @param msg les nouveaux {@link MessageI} à ajouter au channel.
	 */
	public void addMessages(List<MessageI> msgs) {
		// Vérification des préconditions
		assert(msgs != null && msgs.size() > 0);
		
		messages.addAll(msgs);
		
		// Vérification de la postcondition
		assert(messages.containsAll(msgs));
	}
	
	/**
	 * Renvoit la liste des {@link MessageI} du channel.
	 * @return  la liste des {@link MessageI} du channel.
	 */
	public List<MessageI> getMessages() {
		return messages;
	}
	
	/**
	 * Renvoit la liste des URIs des ports clients, abonnés au channel, acceptant le {@code msg} avce le filtre.
	 * @param msg 	un message qui va permettre de récupérer tous les clients du channel acceptant ce filtre.
	 * @return		la liste des clients du channel acceptant ce {@code msg}.
	 */
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
