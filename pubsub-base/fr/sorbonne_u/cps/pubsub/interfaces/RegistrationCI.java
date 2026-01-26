package fr.sorbonne_u.cps.pubsub.interfaces;

// Copyright Jacques Malenfant, Sorbonne Universite.
// Jacques.Malenfant@lip6.fr
//
// This software is a computer program whose purpose is to provide a
// basic component programming model to program with components
// distributed applications in the Java programming language.
//
// This software is governed by the CeCILL-C license under French law and
// abiding by the rules of distribution of free software.  You can use,
// modify and/ or redistribute the software under the terms of the
// CeCILL-C license as circulated by CEA, CNRS and INRIA at the following
// URL "http://www.cecill.info".
//
// As a counterpart to the access to the source code and  rights to copy,
// modify and redistribute granted by the license, users are provided only
// with a limited warranty  and the software's author,  the holder of the
// economic rights,  and the successive licensors  have only  limited
// liability. 
//
// In this respect, the user's attention is drawn to the risks associated
// with loading,  using,  modifying and/or developing or reproducing the
// software by the user in light of its specific status of free software,
// that may mean  that it is complicated to manipulate,  and  that  also
// therefore means  that it is reserved for developers  and  experienced
// professionals having in-depth computer knowledge. Users are therefore
// encouraged to load and test the software's suitability as regards their
// requirements in conditions enabling the security of their systems and/or 
// data to be ensured and,  more generally, to use and operate it in the 
// same conditions as regards security. 
//
// The fact that you are presently reading this means that you have had
// knowledge of the CeCILL-C license and that you accept its terms.

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import java.rmi.RemoteException;

// -----------------------------------------------------------------------------
/**
 * The interface <code>RegistrationCI</code> is offered to users to register on
 * the publication/subscription system and manage their subscriptions to
 * channels.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2026-01-20</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public interface		RegistrationCI
extends		OfferedCI,
			RequiredCI
{
	// -------------------------------------------------------------------------
	// Inner types and classes
	// -------------------------------------------------------------------------

	/**
	 * The interface <code>RegistrationClassI</code> must be implemented by all
	 * registration class enumerations.
	 *
	 * <p><strong>Description</strong></p>
	 * 
	 * <p><strong>Invariants</strong></p>
	 * 
	 * <pre>
	 * invariant	{@code true}	// no more invariant
	 * </pre>
	 * 
	 * <p>Created on : 2026-01-23</p>
	 * 
	 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
	 */
	public static interface	RegistrationClassI
	{
		
	}

	/**
	 * The enumeration <code>RegistrationClass</code> defines the basic
	 * registration classes for the publish/subscribe system.
	 *
	 * <p><strong>Description</strong></p>
	 * 
	 * <p><strong>Implementation Invariants</strong></p>
	 * 
	 * <pre>
	 * invariant	{@code true}	// no more invariant
	 * </pre>
	 * 
	 * <p><strong>Invariants</strong></p>
	 * 
	 * <pre>
	 * invariant	{@code true}	// no more invariant
	 * </pre>
	 * 
	 * <p>Created on : 2026-01-23</p>
	 * 
	 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
	 */
	public static enum RegistrationClass
	implements RegistrationClassI
	{
		/** free access to the system.										*/
		FREE,
		/** standard registration with a low fee.							*/
		STANDARD,
		/** high performance access registration with a higher fee.			*/
		PREMIUM
	}

	// -------------------------------------------------------------------------
	// Signature and default methods
	// -------------------------------------------------------------------------

	/**
	 * return true if {@code receptionPortURI} has already been registered,
	 * otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @return					true if {@code receptionPortURI} has already been registered, otherwise false.
	 * @throws RemoteException	when a RMI error occurs.
	 */
	public boolean		registered(String receptionPortURI)
	throws	RemoteException;

	/**
	 * return true if {@code receptionPortURI} has already been registered
	 * with service class {@code rc}, otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI	URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc				the registration class.
	 * @return					true if {@code receptionPortURI} has already been registered with service class {@code rc}, otherwise false.
	 * @throws RemoteException	when a RMI error occurs.
	 */
	public boolean		registered(String receptionPortURI, RegistrationClass rc)
	throws	RemoteException;

	/**
	 * register on the publication/subscription system passing the URI of the
	 * inbound port offering the component interface {@code ReceivingCI} on
	 * which the user component will receive its messages; the method returns
	 * an URI of an inbound port offering the component interface
	 * {@code PublishingCI} on which the user component will publish its
	 * messages.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * post	{@code registered(receptionPortURI)}
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc							the required registration class.
	 * @return								an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * @throws RemoteException				when a RMI error occurs.
	 * @throws AlreadyRegisteredException	when {@code receptionPortURI} has already been registered.
	 */
	public String		register(String receptionPortURI, RegistrationClass rc)
	throws	RemoteException,
			AlreadyRegisteredException;

	/**
	 * upgrade or downgradeq the registration to the class {@code rc}, returning
	 * a new URI of an inbound port offering the component interface
	 * {@code PublishingCI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code rc != null}
	 * pre	{@code registered(receptionPortURI)}
	 * pre	{@code !registered(receptionPortURI, rc)}
	 * post	{@code registered(receptionPortURI, rc)}
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param rc							the required registration class.
	 * @return								an URI of an inbound port offering the component interface {@code PublishingCI}.
	 * @throws RemoteException				when a RMI error occurs.
	 * @throws AlreadyRegisteredException	when {@code receptionPortURI} has already been registered.
	 */
	public String		modifyServiceClass(
		String receptionPortURI,
		RegistrationClass rc
		) throws	RemoteException,
					AlreadyRegisteredException;

	/**
	 * unregister from the publication/subscription system.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @throws RemoteException				when a RMI error occurs.
	 * @throws UnknownIdentifierException	when {@code receptionPortURI} is not registered.
	 */
	public void			unregister(String receptionPortURI)
	throws	RemoteException,
			UnknownIdentifierException;

	/**
	 * return true if {@code channel} exists, otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel			name of a potential channel.
	 * @return					true if {@code channel} exists, otherwise false.
	 * @throws RemoteException	when a RMI error occurs.
	 */
	public boolean		channelExists(String channel) throws RemoteException;
	
	/**
	 * return true if {@code receptionPortURI} has subscribed to {@code channel},
	 * otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI			URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel					name of an existing channel.
	 * @return							true if {@code receptionPortURI} has subscribed to {@code channel}, otherwise false.
	 * @throws RemoteException			when a RMI error occurs.
	 * @throws UnknownChannelException	when {@code channel} does not exist.
	 */
	public boolean		subscribed(
			String receptionPortURI,
			String channel
			) throws	RemoteException,
						UnknownChannelException;

	/**
	 * subscribe to {@code channel} with filter {@code filter}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						name of an existing channel.
	 * @param filter						filter that accepts the message the component wants to receive.
	 * @throws RemoteException				when a RMI error occurs.
	 * @throws UnknownChannelException		when {@code channel} does not exist.
	 */
	public void			subscribe(
		String receptionPortURI,
		String channel,
		MessageFilterI filter
		) throws	RemoteException,
					UnknownChannelException;

	/**
	 * unsubscribe from {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI					URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel							name of an existing channel.
	 * @throws RemoteException					when a RMI error occurs.
	 * @throws UnknownChannelException			when {@code channel} does not exist.
	 * @throws NotSubscribedChannelException	when the component did not subscribe to {@code channel}.
	 */
	public void			unsubscribe(
		String receptionPortURI,
		String channel
		) throws	RemoteException,
					UnknownChannelException,
					NotSubscribedChannelException;

	/**
	 * modify the filter placed on {@code channel} to {@code filter}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI					URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel							name of an existing channel.
	 * @param filter							filter that accepts the message the component wants to receive.
	 * @return									true if the modification has been performed, otherwise false.
	 * @throws RemoteException					when a RMI error occurs.
	 * @throws UnknownChannelException			when {@code channel} does not exist.
	 * @throws NotSubscribedChannelException	when the component did not subscribe to {@code channel}.
	 */
	public boolean		modifyFilter(
		String receptionPortURI,
		String channel,
		MessageFilterI filter
		) throws	RemoteException,
					UnknownChannelException,
					NotSubscribedChannelException;
}
// -----------------------------------------------------------------------------
