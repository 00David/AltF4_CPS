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

import java.rmi.RemoteException;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;

// -----------------------------------------------------------------------------
/**
 * The interface <code>AdministrationI</code> declares the method signatures
 * used by a publication/subscription system administrator to manage the
 * system; it is also implemented by an administrator plug-in.
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
public interface		AdministrationI
{
	// -------------------------------------------------------------------------
	// Signature and default methods
	// -------------------------------------------------------------------------

	/**
	 * return true if {@code channel} currently exists, otherwise false.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel			name of the channel to be created.
	 * @return					true if {@code channel} currently exists, otherwise false.
	 * @throws RemoteException	when an RMI error occurs.
	 */
	public boolean		channelExists(String channel)
	throws	RemoteException;

	/**
	 * 
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @return								true if the component registered under {@code receptionPortURI} has reached its channel quota.
	 * @throws RemoteException				when an RMI error occurs.
	 * @throws UnknownIdentifierException	when {@code receptionPortURI} does not correspond to a registered component.
	 */
	public boolean		channelQuotaReached(String receptionPortURI)
	throws	RemoteException,
			UnknownIdentifierException;

	/**
	 * create a new channel of the given name.
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
	 * @param channel							name of the channel to be created.
	 * @throws RemoteException					when an RMI error occurs.
	 * @throws UnknownIdentifierException		when {@code receptionPortURI} does not correspond to a registered component.
	 * @throws AlreadyExistingChannelException	when the channel to be created already exists.
	 * @throws ChannelQuotaExceededException	whe, the user tries to exceed its channel quota.
	 */
	public void			createChannel(String receptionPortURI, String channel)
	throws	RemoteException,
			UnknownIdentifierException,
			AlreadyExistingChannelException,
			ChannelQuotaExceededException;

	/**
	 * destroy the given channel when no more messages are currently waiting
	 * to be transmitted.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						name of the channel to be destroyed.
	 * @throws RemoteException				when an RMI error occurs.
	 * @throws UnknownIdentifierException	when {@code receptionPortURI} does not correspond to a registered component.
	 * @throws UnknownChannelException		when the channel to be destroyed does not exist.
	 */
	public void			destroyChannel(String receptionPortURI, String channel)
	throws	RemoteException,
			UnknownIdentifierException,
			UnknownChannelException;

	/**
	 * destroy the given channel immediately, even if the channel has still
	 * messages to be transmitted..
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code receptionPortURI != null && !receptionPortURI.isEmpty()}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param receptionPortURI				URI of the inbound port offering the component interface {@code ReceivingCI}.
	 * @param channel						name of the channel to be destroyed.
	 * @throws RemoteException				when an RMI error occurs.
	 * @throws UnknownIdentifierException	when {@code receptionPortURI} does not correspond to a registered component.
	 * @throws UnknownChannelException		when the channel to be destroyed does not exist.
	 */
	public void			destroyChannelNow(
		String receptionPortURI,
		String channel
		) throws	RemoteException,
					UnknownIdentifierException,
					UnknownChannelException;
}
// -----------------------------------------------------------------------------
