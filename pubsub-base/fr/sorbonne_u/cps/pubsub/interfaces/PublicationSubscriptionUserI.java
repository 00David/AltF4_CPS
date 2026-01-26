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

import java.time.Duration;
import java.util.concurrent.Future;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;

// -----------------------------------------------------------------------------
/**
 * The interface <code>PublicationSubscriptionUserI</code> declares the
 * signatures of methods to be implemented by a
 * publication/subscription system user plug-in. 
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
public interface		PublicationSubscriptionUserI
{
	// -------------------------------------------------------------------------
	// Signature and default methods
	// -------------------------------------------------------------------------

	/**
	 * return true if the component has already been registered, otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	true if the component has already been registered, otherwise false.
	 */
	public boolean		registered();

	/**
	 * register on the publication/subscription system.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code !registered()}
	 * post	{@code registered()}
	 * </pre>
	 *
	 */
	public void			register();

	/**
	 * unregister from the publication/subscription system.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * post	{@code !registered()}
	 * </pre>
	 *
	 */
	public void			unregister();

	/**
	 * return true if {@code channel} exists, otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel	name of a potential channel.
	 * @return			true if {@code channel} exists, otherwise false.
	 */
	public boolean		channelExists(String channel);

	/**
	 * return true if this component already subscribed to {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel					name of an existing channel.
	 * @return							true if this component already subscribed to {@code channel}.
	 * @throws UnknownChannelException	when {@code channel} does not exist.
	 */
	public boolean		subscribed(String channel)
	throws	UnknownChannelException;

	/**
	 * subscribe to {@code channel} with {@code filter}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code !subscribed(channel)}
	 * post	{@code subscribed(channel)}
	 * </pre>
	 *
	 * @param channel					name of an existing channel.
	 * @param filter					filter to be applied to messages to decide if this component must receive them.
	 * @throws UnknownChannelException	when {@code channel} does not exist.
	 */
	public void			subscribe(String channel, MessageFilterI filter)
	throws	UnknownChannelException;

	/**
	 * unsubscribe to {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code subscribed(channel)}
	 * post	{@code !subscribed(channel)}
	 * </pre>
	 *
	 * @param channel					name of an existing channel.
	 * @throws UnknownChannelException	when {@code channel} does not exist.
	 */
	public void			unsubscribe(String channel)
	throws	UnknownChannelException;

	/**
	 * modify the filter associated to the subscription of this component to
	 * {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code subscribed(channel)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel					name of an existing channel.
	 * @param filter					filter to be applied to messages to decide if this component must receive them.
	 * @throws UnknownChannelException	when {@code channel} does not exist.
	 */
	public void			modifyFilter(
		String channel,
		MessageFilterI filter
		) throws UnknownChannelException;

	/**
	 * publish {@code message} on {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code subscribed(channel)}
	 * pre	{@code message != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel					name of an existing channel.
	 * @param message					message to be published on {@code channel}.
	 * @throws UnknownChannelException	when {@code channel} does not exist.
	 */
	public void			publish(String channel, MessageI message)
	throws	UnknownChannelException;

	/**
	 * 
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel	name of an existing channel.
	 * @param message	message to be received from {@code channel}.
	 */
	public void			receive(String channel, MessageI message);

	/**
	 * wait for the next message received from {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code subscribed(channel)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel	name of an existing channel.
	 * @return			the next message received from {@code channel}.
	 */
	public MessageI		waitForNextMessage(String channel);


	/**
	 * get a future on the next message received from {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code subscribed(channel)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel	name of an existing channel.
	 * @return			the next message received from {@code channel}.
	 */
	public Future<MessageI>	getNextMessage(String channel);

	/**
	 * wait for the next message received from {@code channel} for a maximum
	 * duration {@code d}, returning {@code null} if no message has been
	 * received during the next {@code d}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code registered()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel	name of an existing channel.
	 * @param d			maximum duration of the wait.
	 * @return			the next message received from {@code channel} or {@code null} if none.
	 */
	public MessageI		waitForNextMessage(String channel, Duration d);
}
// -----------------------------------------------------------------------------
